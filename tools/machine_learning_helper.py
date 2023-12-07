import numpy as np
import pandas as pd
from sklearn.metrics import make_scorer, mean_squared_error, mean_squared_log_error
from sklearn.model_selection import GridSearchCV
from sklearn.pipeline import Pipeline

from tools.data_helper import filter_data


def train_and_test_pipelines(X_train, y_train, X_test, y_test, pipelines, grid_search_kwargs=None):
    for label, data in pipelines.items():
        data['pipeline'] = Pipeline(data['steps'])

        if 'params_grid' in data.keys():
            # asymmetric_mse_scorer = make_scorer(modified_mean_squared_log_error, greater_is_better=False)
            grid_search = GridSearchCV(
                data['pipeline'],
                data['params_grid'],
                **grid_search_kwargs
                # scoring=asymmetric_mse_scorer,
            )
            grid_search.fit(X_train, y_train)

            best_hyperparams = grid_search.best_params_
            print(f'Best hyperparameters:\n{best_hyperparams}')

            best_CV_score = grid_search.best_score_
            print(f'Best CV accuracy {best_CV_score}')

            best_model = grid_search.best_estimator_
            score = best_model.score(X_test, y_test)
            print(f'{label}: error {score}')

            data['pipeline'] = best_model
        else:
            data['pipeline'].fit(X_train, y_train)
            score = data['pipeline'].score(X_test, y_test)
            print(f'{label}: error {score}')

        y_pred = data['pipeline'].predict(X_test)
        actual_values = y_test * 100
        predicted_values = y_pred * 100
        # Use MSLE to penalize underestimates...need to modify to penalize overestimates
        # Ensure that neither actual_values nor predicted_values contain negative values
        actual_values_adj = np.maximum((1 + actual_values / 100), 0)
        predicted_values_adj = np.maximum((1 + predicted_values / 100), 0)
        # Swap the actual and predicted values to punish overestimates.
        modified_msle = mean_squared_log_error(predicted_values_adj, actual_values_adj)

        print("Modified Mean Squared Logarithmic Error:", modified_msle)

    return pipelines


def modified_mean_squared_log_error(y_true, y_pred):
    # Use MSLE to penalize underestimates...need to modify to penalize overestimates
    # Ensure that neither actual_values nor predicted_values contain negative values
    actual_values_adj = np.maximum((1 + y_true), 0)
    predicted_values_adj = np.maximum((1 + y_pred / 100), 0)
    # Swap the actual and predicted values to punish overestimates.
    modified_msle = mean_squared_log_error(predicted_values_adj, actual_values_adj)

    return modified_msle


def asymmetric_mean_squared_error(y_true, y_pred, multiplier=2):
    error = y_pred - y_true
    penalty = np.where(error > 0, multiplier, 1.0)  # Increase the weight for overestimates
    return np.mean(penalty * (error ** 2))


def asymmetric_squared_error_objective(y_true, y_pred, weight_over=2, weight_under=1):
    # Calculate error
    residual = y_pred - y_true

    # Gradient and Hessian
    grad = np.where(residual > 0, 2 * weight_over * residual, 2 * weight_under * residual)
    hess = np.where(residual > 0, 2 * weight_over, 2 * weight_under)

    return grad, hess


def train_test_split_timeseries(df_dict, target_cols, days_into_future, drop_cols, ohlc_col='close', min_date=None,
                                test_length=1, test_date=None, drop_earnings=None):
    """"""
    df_full_list = []
    df_train_list = []
    df_test_list = []

    index_cols = ['symbol', 'date']
    target_cols_list = list(target_cols.keys())
    for key, df in df_dict.items():
        if min_date:
            df_temp = df.loc[df.index >= min_date].copy()
        else:
            df_temp = df.copy()
        df_temp['symbol'] = key.split('/')[-1]
        df_temp = df_temp.reset_index(drop=False)

        for target_col, info in target_cols.items():
            df_rolling = df_temp[ohlc_col].shift(-days_into_future).rolling(
                window=days_into_future, min_periods=days_into_future)
            function_to_apply = getattr(df_rolling, info['function_name'])
            result = function_to_apply()
            df_temp[target_col] = (result - df_temp[ohlc_col]) / df_temp[ohlc_col]

        # filter data for signal before dropping columns
        df_full_list.append(df_temp)

        df_temp = filter_data(df_temp, signal_rule='bullish_cloud_crossover')

        # remove earnings surprise element...
        if drop_earnings:
            df_temp = df_temp[df_temp['days_since_earnings'] < drop_earnings]

        df_temp.drop(drop_cols, axis=1, inplace=True)
        df_temp = df_temp.dropna(subset=list(target_cols.keys())).copy()

        if test_date:
            df_train = df_temp[df_temp['date'] < test_date]
            df_test = df_temp[df_temp['date'] >= test_date]
        else:
            end_point = len(df_temp)
            X = end_point - test_length
            df_train = df_temp.iloc[:X]
            df_test = df_temp.iloc[X:]

        if not df_train.empty:
            df_train_list.append(df_train)
        if not df_test.empty:
            df_test_list.append(df_test)

    df_train_full = pd.concat(df_train_list, ignore_index=True)
    df_train_full.sort_values(by='date', ascending=True, inplace=True)
    df_train_X_index = df_train_full.loc[:, [col for col in df_train_full.columns if col not in target_cols_list]]
    df_train_y_index = df_train_full[target_cols_list + index_cols]

    df_test_full = pd.concat(df_test_list, ignore_index=True)
    df_test_full.sort_values(by='date', ascending=True, inplace=True)
    df_test_X_index = df_test_full.loc[:, [col for col in df_test_full.columns if col not in target_cols_list]]
    df_test_y_index = df_test_full[target_cols_list + index_cols]

    return {
        'df_full': pd.concat(df_full_list, ignore_index=True),
        'df_train_X_index': df_train_X_index,
        'df_train_y_index': df_train_y_index,
        'df_test_X_index': df_test_X_index,
        'df_test_y_index': df_test_y_index,
        'index_cols': index_cols
    }
