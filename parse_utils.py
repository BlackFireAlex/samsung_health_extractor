import json
import os
import os.path
import sqlite3
from functools import reduce
from glob import glob
import numpy as np
import pandas as pd

sports_codes = {
    14001: 'Swimming',
    1001: 'Walking',
    0: 'Other workout',
    11007: 'Cycling',
    1002: 'Running'
}

global samsung_csv_paths
samsung_data_path = './Samsung Health'


def conv_date(x, col_name=''):
    try:
        if any('day_time' in col for col in x.columns):
            col_name = 'day_time'
            x['day_time'] = pd.to_datetime(
                x[[col for col in x.columns if col_name in col][0]], unit='ms')
        if any('start_time' in col for col in x.columns):
            col_name = 'start_time'
            x['day_time'] = pd.to_datetime(
                x[[col for col in x.columns if col_name in col][0]])
        x['day_time'] = pd.to_datetime(x['day_time'])
    except RuntimeError as e:
        print(e)
        print("Can't find one of these columns")
        print("Column names", x.columns)


# This is for daily sport data
def get_json(s):
    join = os.path.join('**', s + '*.json')
    samsung_json_paths = glob(join, recursive=True)
    json_file = open(samsung_json_paths[0])
    return json.load(json_file)


def get_daily_sports_data(data, day):
    extra_data = None
    sport_counts = dict()
    sports_time = dict()

    try:

        s = str(data[(data['day_time'] == pd.to_datetime(day)) & (data['datauuid_y'] != np.nan)]['datauuid_y'].iloc[0])
        extra_data = get_json(s)

        for d in extra_data['mActivityList']:
            activity_time = d['mActiveTime'] / 1000 / 60
            sport_counts['day_time'] = day
            sports_time['day_time'] = day
            try:
                sport_counts[sports_codes[d['mType']] + ' count'] += 1
            except KeyError:
                sport_counts[sports_codes[d['mType']] + ' count'] = 1
            try:
                sports_time[sports_codes[d['mType']]] += activity_time
            except KeyError:
                sports_time[sports_codes[d['mType']]] = activity_time


    except Exception as e:
        pass

    return sports_time, sport_counts


def get_all_daily_sport_data():
    from datetime import date, timedelta
    all_health_data_concat = get_health_data()

    all_sports_data_dict = dict()
    for sport in sports_codes.values():
        all_sports_data_dict[sport] = []
    all_sports_data_dict['day_time'] = []
    start_date = pd.to_datetime("2021-01-01")
    end_date = all_health_data_concat['day_time'].dt.date.max()
    delta = timedelta(days=1)
    while start_date <= end_date:
        current_date_s = start_date.strftime("%Y-%m-%d")
        daily_sport_dict = get_daily_sports_data(all_health_data_concat, current_date_s)[0]
        all_sports_data_dict['day_time'].append(start_date)
        for k in sports_codes.values():
            if k in daily_sport_dict.keys():
                all_sports_data_dict[k].append(daily_sport_dict[k])
            else:
                all_sports_data_dict[k].append(0)

        start_date += delta

    all_df_merged = pd.DataFrame().from_dict(all_sports_data_dict)
    return all_df_merged


def get_weight_data():
    weight_data_path = os.path.join(samsung_data_path, '**', '*weight*.csv')
    paths = glob(weight_data_path, recursive=True)
    paths.sort(key=len)
    w_data = sam_read_csv(paths[0])
    conv_date(w_data)
    return w_data


def get_samsung_df_merged():
    global samsung_csv_paths

    first_csv = sam_read_csv(samsung_csv_paths[10])
    all_csv_df = {os.path.basename(j).replace('com.samsung.', ''): sam_read_csv(j) for j in samsung_csv_paths}

    key_words_csv = [
        'com.samsung.health.weight'
        , 'com.samsung.shealth.activity.day_summary'
        , 'com.samsung.shealth.calories_burned.details'
    ]

    all_df_of_interest = [v for k, v in all_csv_df.items() if
                          any(x.replace('com.samsung.', '') in k for x in key_words_csv)]

    for i, df in enumerate(all_df_of_interest):
        conv_date(df)

    all_df_merged = reduce(lambda left, right: pd.merge(left, right, on=['day_time'],
                                                        how='outer'), all_df_of_interest)
    return all_df_merged


def get_health_data():
    h_data = get_samsung_df_merged()
    h_data = h_data[[col for col in h_data.columns if "mass" not in col]]
    col_values = h_data.columns
    h_data.columns = [s.replace('com.samsung.shealth.', '') for s in col_values]
    col = [col for col in h_data.columns if len(h_data[col].unique()) >= 2]
    h_data = h_data[col]
    h_data['day_time'] = pd.to_datetime(h_data['day_time'])
    h_data = pd.merge(h_data, get_heart_rate_data(), how="outer", on="day_time")
    return h_data


def extract_activity(record):
    try:
        return record["activity"][0]["activity"][0]["type"]
    except:
        return "MISSING"


def get_heart_rate_data():
    hr_data_path = os.path.join(samsung_data_path, '**', 'com.samsung.shealth.tracker.heart_rate', '**', '*.json')
    hr_file_paths = glob(hr_data_path, recursive=True)
    hr_df = pd.DataFrame(columns=['heart_rate', 'heart_rate_max', 'heart_rate_min', 'start_time', 'end_time'])
    hr_df = hr_df.fillna(0)
    for f in hr_file_paths:
        try:
            with open(f) as file:
                json_hr = json.load(file)
                part_df = pd.DataFrame(json_hr)
                hr_df = pd.concat([hr_df, part_df])

        except ValueError as e:
            print(e)
    hr_df['day_time'] = pd.to_datetime(hr_df['start_time'], unit='ms')
    return hr_df


def get_heart_rate_data_by_hour():
    hr_df = get_heart_rate_data()
    times = pd.DatetimeIndex(hr_df.day_time)
    hr_df = hr_df.groupby([times.hour, times.date]).agg('mean')
    hr_df.reset_index(inplace=True)
    hr_df['day_time'] = pd.to_datetime(
        pd.to_datetime(hr_df['level_1']).dt.strftime('%Y-%m-%d') + ' ' + hr_df['day_time'].astype(str) + ':00')

    return hr_df


# Non aggregated data
def get_heart_rate_data_raw():
    hr_df = get_heart_rate_data()
    return hr_df


def get_exercise_data():
    exercise_paths = os.path.join(samsung_data_path, '**', '*exercise*.csv')
    all_paths = glob(exercise_paths, recursive=True)
    all_paths.sort(key=len)
    cal = sam_read_csv(all_paths[0])
    cal = cal[["com.samsung.health.exercise.time_offset", "com.samsung.health.exercise.start_time",
               "com.samsung.health.exercise.end_time",
               "com.samsung.health.exercise.calorie"]]

    a = cal['com.samsung.health.exercise.time_offset'].astype(str).str[5].astype(int)
    cal["com.samsung.health.exercise.end_time"] = pd.to_datetime(
        cal["com.samsung.health.exercise.end_time"]) + pd.TimedeltaIndex(
        cal['com.samsung.health.exercise.time_offset'].astype(str).str[5].astype(int) - 1, unit='H')

    cal["com.samsung.health.exercise.start_time"] = pd.to_datetime(
        cal["com.samsung.health.exercise.start_time"]) + pd.TimedeltaIndex(
        cal['com.samsung.health.exercise.time_offset'].astype(str).str[5].astype(int) - 1, unit='H')
    return cal


def get_food_intake_data():
    cal_data_path = os.path.join(samsung_data_path, '**', '*food_intake*.csv')
    f_path = glob(cal_data_path, recursive=True)[0]
    cal_data = sam_read_csv(f_path)
    conv_date(cal_data)
    cal_data = cal_data.groupby(cal_data['day_time'].dt.date).agg('sum')
    cal_data.reset_index(inplace=True)
    cal_data = cal_data[cal_data['calorie'] > 0]
    cal_data['day_time'] = pd.to_datetime(cal_data['day_time'])
    return cal_data


def get_burned_calories_data():
    burned_data_path = os.path.join(samsung_data_path, '**', '*burned.details*.csv')
    f_path = glob(burned_data_path, recursive=True)[0]
    burned_data = sam_read_csv(f_path)
    conv_date(burned_data)
    burned_data['total_burned'] = burned_data['com.samsung.shealth.calories_burned.active_calorie'] + burned_data[
        'com.samsung.shealth.calories_burned.rest_calorie'] + burned_data[
                                      'com.samsung.shealth.calories_burned.tef_calorie']
    return burned_data


def get_caloric_balance_daily_data():
    combined_data = pd.merge(get_burned_calories_data(), get_food_intake_data(), on='day_time')
    combined_data['balance'] = combined_data['calorie'] - combined_data['total_burned']
    combined_data = combined_data[
        ['balance', 'total_burned', 'com.samsung.shealth.calories_burned.active_calorie', 'calorie', 'day_time']]
    print("You probably lost about ", combined_data['balance'].sum() / 7250, " during this period !")
    print(combined_data['day_time'].min(), " to ", combined_data['day_time'].max())
    return combined_data


def get_sleep_data():
    global samsung_csv_paths
    sleep_csv = [path for path in samsung_csv_paths if 'sleep.' in path][0]
    sleep_df = sam_read_csv(sleep_csv)

    sleep_df.columns = [col.split('.')[-1] for col in sleep_df.columns]

    time_stamp_cols = ['create_time', 'end_time', 'start_time', 'update_time']

    for col_name in time_stamp_cols:
        sleep_df[col_name] = pd.to_datetime(sleep_df[col_name])

    sleep_df['sleep_duration'] = sleep_df['sleep_duration'].fillna(0)
    sleep_df['sleep_duration'] = sleep_df['sleep_duration'] / 60
    sleep_df.dropna(axis=1, inplace=True, )

    sleep_df['weekday'] = [day.weekday() for day in sleep_df['start_time']]

    sleep_df.sort_values(by=['start_time'], inplace=True)
    sleep_df.reset_index(inplace=True)

    sleep_df['sleep_gap'] = (sleep_df['start_time'].shift(-1) - sleep_df['end_time']) / np.timedelta64(1, 'h')

    sleep_df['Disrupted'] = (sleep_df.sleep_gap < 2) & (sleep_df.sleep_duration < 4)

    sleep_df['weekday'] = sleep_df['weekday'].map({0: "Monday", 1: "Tuesday", 2: "Wednesday",
                                                   3: "Thursday", 4: "Friday", 5: "Saturday", 6: "Sunday"})
    times = pd.DatetimeIndex(sleep_df.start_time)
    sleep_df = sleep_df.groupby(times.date).agg('sum')
    sleep_df.reset_index(inplace=True)

    sleep_df['day_time'] = sleep_df['level_0']
    sleep_df.drop(['level_0', 'index'], axis=1, inplace=True)
    return sleep_df


def sam_read_csv(x):
    return pd.read_csv(x, skiprows=1, sep=",", index_col=False)


def extract_data():
    global samsung_csv_paths
    samsung_base_dir = os.path.join(samsung_data_path)

    samsung_dump_dirs = glob(os.path.join(samsung_base_dir, '*'))
    samsung_dump_dir = os.path.basename(samsung_dump_dirs[0])
    print(len(samsung_dump_dirs), 'dumps found, taking first:', samsung_dump_dir)

    samsung_csv_paths = glob(os.path.join(samsung_base_dir, samsung_dump_dir, '*.csv'))
    print(len(samsung_csv_paths), 'csvs found')
    blood_sugar_data = pd.DataFrame(columns=['empty', 'empty', 'empty'])

    hr_data = get_heart_rate_data_raw().sort_values(by='day_time', ascending=True)
    sleep_data = get_sleep_data()
    daily_sports_data = get_all_daily_sport_data()
    food_intake_data = get_food_intake_data()
    exercise_data = get_exercise_data()
    caloric_balance_data = get_caloric_balance_daily_data().sort_values(by='day_time', ascending=True)
    weight_data = get_weight_data()
    weight_data = weight_data[weight_data['weight'] > 0][['weight', 'day_time']] \
        .sort_values(by='day_time', ascending=False)

    df_data_list = [blood_sugar_data, hr_data, sleep_data, daily_sports_data,
                    food_intake_data, exercise_data, caloric_balance_data, weight_data]
    df_name_list = ['blood_sugar_data', 'hr_data', 'sleep_data',
                    'daily_sports_data', 'food_intake_data', 'exercise_data', 'caloric_balance_data', 'weight_data']

    for i, d in enumerate(df_data_list):
        try:
            d.to_csv('./' + str(df_name_list[i]) + '.csv')
        except Exception as e:
            print(e)


if __name__ == "__main__":
    extract_data()
