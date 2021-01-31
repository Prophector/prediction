import multiprocessing
import os
import time

import pandas as pd
from fbprophet import Prophet
from fbprophet.diagnostics import cross_validation, performance_metrics
from fbprophet.plot import add_changepoints_to_plot
from fbprophet.plot import plot_cross_validation_metric

if os.getenv('DOCKER_ACTIVE') is None:
    multiprocessing.set_start_method("fork")


def process_job(conn, job):
    assert job['type'].lower() in ['cases', 'deaths', 'tests']

    print(f"{time.strftime('%H:%M:%S')} Starting job={job}")

    data = query_data(conn, job)

    df = prepare_data(job, data)
    m = create_model(job)

    m.fit(df)

    # predict a third into the future of what we looked back
    future_days = round(job['days_to_look_back'] / 3)
    future = m.make_future_dataframe(periods=future_days)

    future['cap'] = df['cap'][0]
    forecast = m.predict(future)

    # region debug
    if os.getenv('DOCKER_ACTIVE') is None:
        fig = m.plot(forecast)
        add_changepoints_to_plot(fig.gca(), m, forecast)
        fig.savefig(f"../job/prediction-{job['id']}.png")
    # endregion

    change_points = m.changepoints.dt.date.tolist()
    store_prediction(conn, job, forecast, change_points)

    # cross validate and create score
    if job['with_score']:
        # compute period to have 5-6 simulated forecasts
        horizon = pd.Timedelta("14 days")
        initial = horizon * 3
        period = (df.iloc[-1]['ds'] - df.iloc[0]['ds'] - horizon - initial) / 5

        df_cv = cross_validation(m, initial=initial, horizon=horizon, period=period, parallel='processes')
        df_p = performance_metrics(df_cv)

        # region debug
        if os.getenv('DOCKER_ACTIVE') is None:
            fig = plot_cross_validation_metric(df_cv, metric='mape')
            fig.savefig(f"../job/score-{job['id']}.png")
        # endregion

        score = df_p.iloc[-1]['mape']
        store_score(conn, job, score)


def query_data(conn, job):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT date as ds, " + job['type'].lower() +
            " as y FROM pro_datapoint "
            "WHERE country_id = %s AND main_region = true AND date > (now() - INTERVAL '%s DAY') "
            "ORDER BY date",
            (job['country_id'], job['days_to_look_back']))
        records = cur.fetchall()
        return records


def prepare_data(job, data):
    df = pd.DataFrame(data=data, columns=['ds', 'y']) \
        .set_index(keys='ds') \
        .squeeze()

    if job['display_type'] == 'daily':
        df = df.diff()

    if job['smoothing'] > 0:
        df = df.rolling(job['smoothing']).mean()

    if job['rolling_sum_window'] > 1:
        df = df.rolling(job['rolling_sum_window']).sum()

    df = df.to_frame().reset_index()
    df['cap'] = round(df['y'].max() * 10)

    return df


def create_model(job):
    change_points = None
    if len(job['change_points']) > 0:
        change_points = job['change_points']

    m = Prophet(
        growth='logistic',
        changepoints=change_points,
        n_changepoints=job['num_change_points'],
        changepoint_range=job['change_point_range'],
        yearly_seasonality=False,
        weekly_seasonality='auto',
        daily_seasonality=False,
        holidays=None,
        seasonality_mode=job['seasonality_mode'],
        changepoint_prior_scale=job['change_point_prior_scale'],
        holidays_prior_scale=job['holidays_prior_scale'],
        seasonality_prior_scale=job['seasonality_prior_scale'],
        mcmc_samples=0
    )

    if job['add_country_holidays']:
        try:
            m.add_country_holidays(country_name=job['country_iso_code'])
        except Exception as ex:
            print(f"Adding country holidays for {job['country_iso_code']} failed: {ex}")

    return m


def store_prediction(conn, job, forecast, change_points):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM pro_datapoint_prediction WHERE model_id = %s", [job['model_id']])

        for _, row in forecast.iterrows():
            is_change_point = row['ds'] in change_points
            cur.execute(
                "INSERT INTO pro_datapoint_prediction "
                "(model_id, date, upper_bound, y_hat, lower_bound, is_change_point) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (job['model_id'], row['ds'], row['yhat_upper'], row['yhat'], row['yhat_lower'], is_change_point))


def store_score(conn, job, score):
    with conn.cursor() as cur:
        cur.execute("UPDATE PRO_PROPHET_JOB SET score = %s WHERE id = %s", [score, job['id']])
