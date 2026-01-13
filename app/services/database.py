"""
Сервис БД как у Николая, нужно будет на рефакторинг подключений к бд
"""
import asyncio
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

import aioodbc

from app.exceptions.sql_database import DatabaseError
from app.services.base_reconnectable import ReconnectableService
from app.utils.logging import get_logger
from config import settings

logger = get_logger()


class DatabaseService(ReconnectableService):
    # Индикаторы закрытого/протухшего соединения (вынесено в константу класса)
    CLOSED_CONN_INDICATORS = [
        'closed', 'lost', 'reset', 'broken pipe', 'network', 'login timeout',
        "the cursor's connection has been closed", 'cannot acquire connection after closing pool',
        'sqletran', 'hy010', 'function sequence error', 'invalid connection',
        'connection is closed', 'connection closed', 'connection lost',
        "generator didn't stop after athrow", 'aioodbc.connection'
    ]
    SQL_TIMEOUT_INDICATORS = [
        'timeout', 'lock', 'deadlock', 'blocked',
        'превышено', 'блокировк', 'ожидания',
        '1222', '1205'
    ]

    # Таймауты для обнаружения прокисших соединений
    # Увеличены для учета очень медленных БД после простоя (даже SELECT 1 может выполняться долго)
    # Валидация - это легкий запрос SELECT 1, но на медленной БД после простоя он может выполняться долго
    # Тяжелые запросы/процедуры выполняются после валидации и имеют свои таймауты (query_timeout)
    CONN_VALIDATION_TIMEOUT = 40.0  # Таймаут валидации соединения (40 секунд - достаточно для медленной БД)
    CONN_VALIDATION_FETCH_TIMEOUT = 5.0  # Таймаут для fetchone при валидации (5 секунд)
    POOL_ACQUIRE_TIMEOUT = 5.0  # Сколько ждём acquire из пула
    QUERY_GUARD_GRACE_SECONDS = 5.0  # Допуск к таймауту запроса (fetch/cleanup)

    # Примечание: прокисшие соединения обнаруживаются через ошибки сразу, без ожидания таймаута
    # Таймаут нужен только для очень медленных БД после простоя

    def __init__(self):
        super().__init__("Database")
        self.connection_string = self._build_connection_string()
        self._pool: Optional[aioodbc.Pool] = None
        self._pool_lock: Optional[asyncio.Lock] = None  # Lazy initialization
        self._pool_created_at: Optional[float] = None  # Время создания пула для проверки возраста

    def _get_pool_lock(self) -> asyncio.Lock:
        """Получить lock с lazy initialization"""
        if self._pool_lock is None:
            self._pool_lock = asyncio.Lock()
            logger.info("Создан блокировка пула подключений к базе данных")
        return self._pool_lock

    def _build_connection_string(self) -> str:  # noqa
        """Формирует строку подключения к MSSQL"""
        return (
            f"Driver={settings.DB_DRIVER};"
            f"Server={settings.DB_SERVER},{settings.DB_PORT};"
            f"Database={settings.DB_NAME};"
            f"UID={settings.DB_USER};"
            f"PWD={settings.DB_PASSWORD};"
            "Encrypt=yes;"
            "TrustServerCertificate=yes;"
            "Connection Timeout=40;"
        )

    async def _connect(self):
        """Создает пул соединений"""
        if self._pool is not None:
            logger.warning("Пул подключений уже существует, создание пропущено")
            return

        logger.info("Создаётся пул подключений к базе данных (minsize=1, maxsize=10, timeout=40s)...")
        self._pool = await aioodbc.create_pool(
            dsn=self.connection_string,
            minsize=1,
            maxsize=10,
            timeout=40
        )
        self._pool_created_at = time.time()  # Запоминаем время создания пула
        logger.info(f"Пул подключений к базе данных успешно создан ({self._get_pool_stats()})")

    async def _disconnect(self):
        """Закрывает пул соединений с таймаутом"""
        if self._pool is None:
            return

        try:
            self._pool.close()
            # Ждем закрытия с таймаутом - не зависаем навечно
            await asyncio.wait_for(self._pool.wait_closed(), timeout=5.0)
            logger.info("Пул подключений к базе данных успешно закрыт")
        except asyncio.TimeoutError:
            logger.warning("Закрытие пула заняло более 5 секунд, принудительная очистка")
        except Exception as e:
            logger.error(f"Ошибка при закрытии пула подключений: {e}")
        finally:
            self._pool = None
            self._pool_created_at = None

    async def recreate_pool(self, timeout: float = 10.0) -> None:
        """Безопасно пересоздаёт пул избегая race conditions

        Args:
            timeout: Максимальное время ожидания lock (по умолчанию 10 сек)
        """

        async def _do_recreate():
            async with self._get_pool_lock():
                # Проверяем, не пересоздан ли пул уже другим корутином
                # Проверяем и что пул существует, и что он не закрыт, и что он не слишком старый
                if self._pool is not None and not self._is_pool_closed():
                    # Проверяем возраст пула - если он свежий, значит уже пересоздан другим корутином
                    if not self._is_pool_too_old(max_age_hours=settings.DB_POOL_MAX_AGE_HOURS):
                        logger.info("Пул подключений уже был пересоздан другим корутином, пересоздание не требуется")
                        return
                    # Если пул старый, продолжаем пересоздание

                logger.info("Выполняется пересоздание пула подключений к базе данных...")
                old_pool = self._pool
                old_pool_id = id(old_pool) if old_pool else None

                # Создаем новый пул
                try:
                    start_time = time.time()
                    new_pool = await aioodbc.create_pool(
                        dsn=self.connection_string,
                        minsize=1,
                        maxsize=10,
                        timeout=40
                    )
                    creation_time = time.time() - start_time

                    # Атомарно меняем пул
                    self._pool = new_pool
                    self._pool_created_at = time.time()  # Обновляем время создания пула
                    logger.info(
                        f"Новый пул подключений успешно создан за {creation_time:.2f}s (pool_id={id(new_pool)})")

                    # Закрываем старый пул в фоне если он был
                    if old_pool is not None:
                        logger.info(f"Фоновое закрытие старого пула подключений запланировано (pool_id={old_pool_id})")
                        asyncio.create_task(self._close_pool_background(old_pool, old_pool_id))

                except Exception as e:
                    logger.error(f"Не удалось пересоздать пул подключений: {e}")
                    # Если не удалось создать новый пул, оставляем старый
                    raise

        try:
            # Ждем lock с таймаутом - не виснем если другой запрос уже пересоздает пул
            await asyncio.wait_for(_do_recreate(), timeout=timeout)
        except asyncio.TimeoutError:
            # Не смогли получить lock за timeout - возможно другой поток пересоздает пул
            logger.warning(
                f"Не удалось получить блокировку для пересоздания пула за {timeout}s, вероятно, пул уже пересоздаётся другим корутином")
            # Проверяем состояние пула после таймаута
            if self._is_pool_closed():
                raise DatabaseError("Таймаут пересоздания пула - пул остаётся закрытым, продолжение работы невозможно")
            # Если пул не закрыт, возможно другой корутин успел пересоздать его

    async def _close_pool_background(self, pool: aioodbc.Pool, pool_id: int = None) -> None:  # noqa
        """Закрывает пул в фоне без блокировки"""
        pool_id_str = f"pool_id={pool_id}" if pool_id else "unknown pool"
        try:
            start_time = time.time()
            pool.close()
            await asyncio.wait_for(pool.wait_closed(), timeout=10.0)
            closure_time = time.time() - start_time
            logger.info(f"Старый пул подключений успешно закрыт в фоне за {closure_time:.2f}s ({pool_id_str})")
        except asyncio.TimeoutError:
            logger.warning(
                f"Фоновое закрытие пула превысило 10 секунд, пул будет очищен сборщиком мусора ({pool_id_str})")
        except Exception as e:
            logger.error(f"Ошибка при фоновом закрытии старого пула подключений ({pool_id_str}): {e}")

    async def _health_check(self) -> bool:
        """Проверка здоровья БД"""
        if not self._pool:
            return False

        try:
            async with self._pool.acquire() as conn:
                # Устанавливаем LOCK_TIMEOUT и ARITHABORT для health check
                try:
                    async with conn.cursor() as setup_cursor:
                        await setup_cursor.execute("SET LOCK_TIMEOUT 1000")
                        # Устанавливаем ARITHABORT для оптимизации
                        if settings.DB_ARITHABORT:
                            await setup_cursor.execute("SET ARITHABORT ON")
                except Exception:  # noqa
                    pass  # Игнорируем ошибку установки timeout/arithabort

                async with conn.cursor() as cursor:
                    # Проверяем базовое соединение к БД
                    # Не проверяем конкретные таблицы - блокировки таблиц это временная ситуация,
                    # которая не означает что БД недоступна в целом
                    await cursor.execute("SELECT 1")
                    result = await cursor.fetchone()
                    return result[0] == 1 if result else False
        except Exception as e:
            # Логируем timeout/lock ошибки как warning, не как error
            error_msg = str(e).lower()
            timeout_indicators = ['timeout', 'lock', 'deadlock', 'blocked', 'превышено', 'блокировк', '1222', '1205']
            if any(indicator in error_msg for indicator in timeout_indicators):
                logger.warning(
                    f"Проверка доступности базы данных заблокирована (возможна долгосрочная транзакция): {str(e)}")
            return False

    def _is_pool_closed(self) -> bool:
        """Проверяет, закрыт ли пул"""
        if self._pool is None:
            return True
        try:
            # aioodbc Pool имеет атрибут _closed
            return getattr(self._pool, '_closed', False)
        except Exception:
            # Если не удалось проверить состояние, считаем пул закрытым для безопасности
            return True

    def _get_pool_stats(self) -> str:
        """Получает минимальную статистику пула (только для диагностики ошибок)"""
        if self._pool is None:
            return "pool=None"
        try:
            pool_id = id(self._pool)
            closed = getattr(self._pool, '_closed', False)
            age_hours = None
            if self._pool_created_at is not None:
                age_hours = (time.time() - self._pool_created_at) / 3600
            return f"pool_id={pool_id}, closed={closed}, age={age_hours:.1f}h" if age_hours is not None else f"pool_id={pool_id}, closed={closed}"
        except Exception:  # noqa
            return "pool_stats_unavailable"

    def _is_pool_too_old(self, max_age_hours: float = 7.0) -> bool:
        """Проверяет, не слишком ли старый пул

        Args:
            max_age_hours: Максимальный возраст пула в часах (по умолчанию 7 часов)

        Returns:
            True если пул старше max_age_hours, False иначе
        """
        if self._pool is None or self._pool_created_at is None:
            return False

        age_hours = (time.time() - self._pool_created_at) / 3600
        return age_hours > max_age_hours

    async def _ensure_service_available(self) -> None:
        """Проверяет circuit breaker и при необходимости инициирует переподключение"""
        try:
            await self.ensure_connection()
        except RuntimeError as e:
            logger.warning(f"Схема circuit breaker для базы данных сейчас в открытом состоянии: {str(e)}")
            self.schedule_background_reconnect()
            raise DatabaseError("Сервис базы данных временно недоступен: circuit breaker в открытом состоянии")
        except Exception as e:
            logger.error(f"Не удалось гарантировать доступность подключения к базе данных: {str(e)}")
            self._record_failure()
            self.schedule_background_reconnect()
            raise DatabaseError(f"Ошибка подключения к базе данных: {str(e)}")

    async def _ensure_pool_ready_for_use(self) -> None:
        """Гарантирует, что пул существует, не закрыт и не протух"""
        if self._pool is None or self._is_pool_closed():
            logger.warning(
                f"Пул подключений не готов к использованию ({self._get_pool_stats()}), выполняется пересоздание...")
            await self._recreate_pool_with_reason("pool_not_ready")
            if self._is_pool_closed():
                raise DatabaseError("Не удалось пересоздать пул подключений — пул остаётся закрытым после пересоздания")
            return

        if self._is_pool_too_old(max_age_hours=settings.DB_POOL_MAX_AGE_HOURS):
            pool_age_hours = (time.time() - self._pool_created_at) / 3600 if self._pool_created_at else 0
            logger.info(
                f"Пул подключений слишком старый ({pool_age_hours:.1f}h > {settings.DB_POOL_MAX_AGE_HOURS}h), выполняется плановое пересоздание...")
            await self._recreate_pool_with_reason("pool_too_old")
            if self._is_pool_closed():
                raise DatabaseError("Не удалось пересоздать пул подключений — пул остаётся закрытым после пересоздания")

    async def _validate_connection(self, conn: aioodbc.Connection) -> None:
        """Проверяет, что соединение живое (SELECT 1)"""
        async with conn.cursor() as ping_cursor:
            await asyncio.wait_for(ping_cursor.execute("SELECT 1"), timeout=self.CONN_VALIDATION_TIMEOUT)
            await asyncio.wait_for(ping_cursor.fetchone(), timeout=self.CONN_VALIDATION_FETCH_TIMEOUT)

    async def _setup_connection_session(self, conn: aioodbc.Connection, query_timeout: int) -> None:
        """Настраивает параметры сессии (LOCK_TIMEOUT, ARITHABORT)"""
        lock_timeout_ms = query_timeout * 1000
        try:
            async with conn.cursor() as cursor:
                await cursor.execute(f"SET LOCK_TIMEOUT {lock_timeout_ms}")
                if settings.DB_ARITHABORT:
                    await cursor.execute("SET ARITHABORT ON")
                    logger.debug("Для соединения установлен ARITHABORT ON")
                else:
                    await cursor.execute("SET ARITHABORT OFF")
                    logger.debug("Для соединения установлен ARITHABORT OFF")
        except Exception as setup_error:
            logger.warning(f"Не удалось задать параметры LOCK_TIMEOUT/ARITHABORT: {str(setup_error)}")

    def _collect_exception_messages(self, exc: Exception) -> list[str]:
        """Собирает все сообщения из цепочки исключений"""
        messages: list[str] = []
        seen: set[int] = set()
        current: Optional[BaseException] = exc
        while current and id(current) not in seen:
            seen.add(id(current))
            try:
                messages.append(str(current))
            except Exception:
                messages.append(current.__class__.__name__)
            if getattr(current, "__cause__", None):
                current = current.__cause__
                continue
            current = getattr(current, "__context__", None)
        return messages

    def _is_connection_exception(self, exc: Exception) -> bool:
        """Определяет, описывает ли исключение проблемы с соединением/таймаутом"""
        if isinstance(exc, asyncio.TimeoutError):
            return True
        for message in self._collect_exception_messages(exc):
            lower_msg = message.lower()
            if any(indicator in lower_msg for indicator in self.CLOSED_CONN_INDICATORS):
                return True
        return False

    def _is_sql_timeout_error(self, exc: Exception) -> bool:
        """Определяет, указывает ли исключение на lock/timeout внутри SQL Server"""
        for message in self._collect_exception_messages(exc):
            lower_msg = message.lower()
            if any(indicator in lower_msg for indicator in self.SQL_TIMEOUT_INDICATORS):
                return True
        return False

    def _query_guard_timeout(self, query_timeout: int) -> float:
        """Возвращает верхнюю границу таймаута для run_query (запрос + освобождение курсора)"""
        return max(query_timeout, 1) + self.QUERY_GUARD_GRACE_SECONDS

    async def _recreate_pool_with_reason(self, reason: str) -> None:
        """Обёртка для recreate_pool с логированием причины"""
        logger.info(f"metric=pool_recreate_reason reason={reason} stats=({self._get_pool_stats()})")
        await self.recreate_pool()

    def _log_metric(self, name: str, **fields) -> None:
        """Выводит структурированный лог для диагностик"""
        fields_payload = " ".join(f"{key}={value}" for key, value in fields.items())
        logger.info(f"metric={name} {fields_payload}".strip())

    async def execute_with_timeout(
            self,
            cursor: aioodbc.Cursor,
            query: str,
            params: tuple = None,
            timeout: float = 40.0
    ) -> None:
        """Выполняет запрос с таймаутом

        Args:
            cursor: Курсор для выполнения запроса
            query: SQL запрос
            params: Параметры запроса
            timeout: Таймаут выполнения в секундах (по умолчанию 40)

        Raises:
            asyncio.TimeoutError: Если запрос не выполнился за timeout
        """
        if params:
            await asyncio.wait_for(cursor.execute(query, params), timeout=timeout)
        else:
            await asyncio.wait_for(cursor.execute(query), timeout=timeout)

    async def run_query(
            self,
            query_func,
            query_timeout: int = 40,
            max_retries: int = 2
    ):
        """Выполняет функцию-запрос с автоматическим повтором при таймаутах и обрывах соединения."""
        operation_name = getattr(query_func, "__name__", "DB query")
        last_error: Optional[Exception] = None

        for attempt in range(1, max_retries + 1):
            attempt_label = f"{operation_name} (попытка {attempt}/{max_retries})"
            try:
                async with self.get_connection(query_timeout=query_timeout) as conn:
                    async with conn.cursor() as cursor:
                        guard_timeout = self._query_guard_timeout(query_timeout)
                        logger.debug(f"{attempt_label}: выполнение с таймаутом {guard_timeout:.1f}s")
                        return await asyncio.wait_for(
                            query_func(conn, cursor),
                            timeout=guard_timeout
                        )
            except asyncio.TimeoutError as exc:
                logger.warning(f"{attempt_label} завершилась по таймауту после {query_timeout}s")
                last_error = DatabaseError(f"Таймаут выполнения запроса к базе данных после {query_timeout}s")
                if attempt < max_retries:
                    if attempt == 1:
                        self._log_metric("first_attempt_retry", operation=operation_name, reason="timeout")
                    logger.info("Повторный запуск запроса после таймаута...")
                    continue
                raise last_error from exc
            except DatabaseError as exc:
                last_error = exc
                if self._is_connection_exception(exc) and attempt < max_retries:
                    if attempt == 1:
                        self._log_metric("first_attempt_retry", operation=operation_name, reason="connection")
                    logger.warning(f"{attempt_label} завершилась с ошибкой соединения: {exc}. Повторная попытка...")
                    continue
                if self._is_sql_timeout_error(exc):
                    raise DatabaseError(
                        "База данных временно недоступна: таймаут выполнения запроса (возможна долгосрочная транзакция)") from exc
                raise
            except Exception as exc:
                last_error = exc
                if self._is_connection_exception(exc) and attempt < max_retries:
                    if attempt == 1:
                        self._log_metric("first_attempt_retry", operation=operation_name, reason="connection")
                    logger.warning(f"{attempt_label} завершилась с ошибкой соединения: {exc}. Повторная попытка...")
                    continue
                if self._is_sql_timeout_error(exc):
                    raise DatabaseError(
                        "База данных временно недоступна: таймаут выполнения запроса (возможна долгосрочная транзакция)") from exc
                logger.error(f"{attempt_label} завершилась с ошибкой: {exc}")
                raise

        if last_error is not None:
            raise last_error
        raise DatabaseError(f"{operation_name} завершилась с ошибкой без детального описания")

    async def execute_query_with_retry(
            self,
            query_func,
            query_timeout: int = 40,
            max_retries: int = 2
    ):
        """Совместимость со старым API."""
        return await self.run_query(query_func, query_timeout=query_timeout, max_retries=max_retries)

    @asynccontextmanager
    async def get_connection(self, query_timeout: int = 5) -> AsyncGenerator[aioodbc.Connection, None]:
        """Возвращает соединение с БД, гарантируя автоматическое закрытие/пересоздание пула."""
        guard = _ConnectionGuard(self, query_timeout)
        async with guard as conn:
            yield conn


# Внутренний guard, который следит за соединением и состоянием пула
class _ConnectionGuard:
    MAX_ACQUIRE_ATTEMPTS = 2

    def __init__(self, service: "DatabaseService", query_timeout: int):
        self.service = service
        self.query_timeout = query_timeout
        self.conn: Optional[aioodbc.Connection] = None
        self.pool_used: Optional[aioodbc.Pool] = None
        self.pool_needs_recreation = False
        self.started_at = time.monotonic()

    async def __aenter__(self) -> aioodbc.Connection:
        await self.service._ensure_service_available()

        for attempt in range(1, self.MAX_ACQUIRE_ATTEMPTS + 1):
            await self.service._ensure_pool_ready_for_use()
            self.pool_used = self.service._pool

            if self.pool_used is None:
                await asyncio.sleep(0)
                continue

            pool_id = id(self.pool_used)
            try:
                acquire_start = time.monotonic()
                self.conn = await asyncio.wait_for(
                    self.pool_used.acquire(),
                    timeout=self.service.POOL_ACQUIRE_TIMEOUT
                )
                acquire_time = time.monotonic() - acquire_start
                logger.info(f"Соединение с базой данных получено за {acquire_time:.3f}s (pool_id={pool_id})")

                await self.service._validate_connection(self.conn)
                await self.service._setup_connection_session(self.conn, self.query_timeout)
                return self.conn
            except Exception as exc:
                if await self._handle_acquire_exception(exc, attempt, pool_id):
                    continue
                raise

        raise DatabaseError("Не удалось получить соединение с базой данных после нескольких попыток")

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        if exc_type is not None:
            err = exc if isinstance(exc, Exception) else None
            if err and (isinstance(err, asyncio.TimeoutError) or self.service._is_connection_exception(err)):
                logger.warning(
                    "Соединение с БД помечено как невалидное из-за ошибки операции, соединение будет закрыто")
                await self._invalidate_connection()
                self.pool_needs_recreation = True

        await self._release_connection()

        if self.pool_needs_recreation:
            logger.info("Выполняется пересоздание пула подключений по сигналу guard...")
            try:
                await self.service._recreate_pool_with_reason("connection_guard")
            except Exception as recreate_err:
                logger.warning(f"Не удалось пересоздать пул после выхода из guard: {recreate_err}")

        return False

    async def _handle_acquire_exception(self, exc: Exception, attempt: int, pool_id: int) -> bool:
        """Обрабатывает ошибки при acquire/валидации. Возвращает True, если нужно повторить попытку."""
        is_timeout = isinstance(exc, asyncio.TimeoutError)
        is_connection_issue = self.service._is_connection_exception(exc)

        if is_timeout or is_connection_issue:
            reason = "timeout" if is_timeout else str(exc)
            logger.warning(
                f"Ошибка при получении/проверке соединения ({reason}) на попытке {attempt}/{self.MAX_ACQUIRE_ATTEMPTS} "
                f"(pool_id={pool_id}, stats={self.service._get_pool_stats()})"
            )
            await self._invalidate_connection()
            self.pool_needs_recreation = True

            if attempt >= self.MAX_ACQUIRE_ATTEMPTS:
                raise DatabaseError(f"Не удалось получить соединение с базой данных: {reason}") from exc

            await self.service._recreate_pool_with_reason("acquire_failure")
            self.pool_used = None
            return True

        # Неизвестная ошибка - пробрасываем выше
        return False

    async def _invalidate_connection(self) -> None:
        """Закрывает текущее соединение, не возвращая его в пул"""
        if self.conn is None:
            return
        try:
            await self.conn.close()
        except Exception as close_err:
            logger.debug(f"Ошибка при закрытии соединения с базой данных: {close_err}")
        finally:
            self.conn = None

    async def _release_connection(self) -> None:
        """Возвращает соединение в пул или закрывает его"""
        if self.conn is None:
            return

        total_time = time.monotonic() - self.started_at
        pool_id = id(self.pool_used) if self.pool_used else None

        if self.pool_used and not getattr(self.pool_used, "_closed", False) and not self.pool_needs_recreation:
            try:
                await self.pool_used.release(self.conn)
                logger.info(
                    f"Соединение с базой данных возвращено в пул, время использования {total_time:.3f}s (pool_id={pool_id})")
            except Exception as release_err:
                logger.warning(f"Ошибка при возврате соединения в пул (pool_id={pool_id}): {release_err}")
                await self._invalidate_connection()
                self.pool_needs_recreation = True
        else:
            reason = "пул закрыт" if self.pool_used and getattr(self.pool_used, "_closed",
                                                                False) else "соединение помечено как невалидное"
            logger.debug(
                f"Соединение закрывается напрямую ({reason}), время использования {total_time:.3f}s (pool_id={pool_id})")
            await self._invalidate_connection()
        self.conn = None
