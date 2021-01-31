import os
import traceback
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor

from process_job import process_job


def watch_queue():
    db_host = os.getenv('DB_HOST') or 'localhost'
    db_port = os.getenv('DB_PORT') or '5432'
    db_name = os.getenv('DB_NAME') or 'postgres'
    db_username = os.getenv('DB_USERNAME') or 'postgres'
    db_password = os.getenv('DB_PASSWORD') or 'postgres'
    with psycopg2.connect(f"host={db_host} port={db_port} dbname={db_name} user={db_username} password={db_password}") as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT "
                "j.id as id, "
                "j.model_id as model_id, "
                "j.with_score as with_score, "
                "m.country_id as country_id, "
                "c.iso_code as country_iso_code, "
                "m.smoothing as smoothing, "
                "m.type as type, "
                "m.display_type as display_type, "
                "m.rolling_sum_window as rolling_sum_window, "
                "m.days_to_look_back as days_to_look_back, "
                "m.num_change_points as num_change_points, "
                "m.change_point_range as change_point_range, "
                "m.seasonality_mode as seasonality_mode, "
                "m.change_point_prior_scale as change_point_prior_scale, "
                "m.holidays_prior_scale as holidays_prior_scale, "
                "m.seasonality_prior_scale as seasonality_prior_scale, "
                "m.add_country_holidays as add_country_holidays "
                "FROM pro_prophet_job j "
                "INNER JOIN pro_prophet_model m ON j.model_id = m.id "
                "INNER JOIN pro_country c ON m.country_id = c.id "
                "WHERE j.job_status = 'IN_QUEUE'")
            pending_jobs = cur.fetchall()

            # fetch change points (if any) and populate job
            for job in pending_jobs:
                cur.execute("SELECT date FROM pro_prophet_model_change_point WHERE model_id = %s", [job['model_id']])
                # job['change_points'] = cur.fetchall().apply(lambda x: x['date'])
                job['change_points'] = [x['date'].strftime('%Y-%m-%d') for x in cur.fetchall()]

        with conn.cursor() as cur:
            for job in pending_jobs:
                run_job(cur, conn, job)


def run_job(cur, conn, job):
    final_status = 'FAILED'
    error_reason = ''
    try:
        cur.execute("UPDATE pro_prophet_job SET job_status = 'PROCESSING', started_timestamp = %s "
                    "WHERE id = %s AND job_status = 'IN_QUEUE'", [datetime.now(), job['id']])
        if cur.rowcount != 1:
            # some other process started processing this job -> skip
            return
        conn.commit()

        process_job(conn, job)
        final_status = 'DONE'
    except Exception as ex:
        print(f"Job(jobId={job['id']}) failed: {ex}")
        error_reason = str(ex)
        print(traceback.format_exc())
    finally:
        print(f"Job(jobId={job['id']}) finished")
        cur.execute("UPDATE pro_prophet_job "
                    "SET job_status = %s, finished_timestamp = %s, error_reason = %s "
                    "WHERE id = %s", (final_status, datetime.now(), error_reason, job['id']))
        conn.commit()
