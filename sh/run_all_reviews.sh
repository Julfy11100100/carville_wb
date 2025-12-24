#!/bin/bash
cd /srv/test/mp_wb_manager
/usr/bin/docker compose run --rm wb_manager python -m app.scripts.get_all_reviews
