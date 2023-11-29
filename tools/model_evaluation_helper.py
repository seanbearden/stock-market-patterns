import matplotlib.pyplot as plt
import numpy as np
from sklearn.preprocessing import OneHotEncoder


def find_first_above_threshold(lst, threshold):
    for index, value in enumerate(lst):
        if value > threshold:
            return index
    return None  # or an appropriate value indicating no such element was found


def find_first_below_threshold(lst, threshold):
    for index, value in enumerate(lst):
        if value < threshold:
            return index
    return None  # or an appropriate value indicating no such element was found


def get_accuracy_fig(predicted_values, actual_values):
    fig, ax = plt.subplots()  # Create a figure and an axes.
    ax.scatter(predicted_values, actual_values)
    ax.plot([actual_values.min(), actual_values.max()], [actual_values.min(), actual_values.max()], 'k--', lw=2)
    ax.set_title('True Values vs. Predictions')
    ax.set_xlabel('Predicted Values')
    ax.set_ylabel('True Values')

    return fig


def get_actual_vs_threshold_prob_fig(predicted_values, actual_values, x=None):
    actual_vs_thresh_prob = []

    if x is None:
        x = np.linspace(0, 10, 101)

    for prediction_threshold in x:
        # what percentage of predicted_values above prediction_threshold had an actual_value above prediction_threshold
        actual_vs_thresh_prob.append(100 * ((actual_values > prediction_threshold * 1.0) & (predicted_values >
                                                                                            prediction_threshold)).mean())
    fig, ax = plt.subplots()  # Create a figure and an axes.

    ax.plot(x, actual_vs_thresh_prob)
    prob_threshold = 50
    prob_threshold_x = x[find_first_below_threshold(actual_vs_thresh_prob, prob_threshold)]
    ax.axvline(x=prob_threshold_x, color='r', linestyle='-')  # Adding a vertical line
    # Annotating the horizontal line
    annotation = f'{prob_threshold}% of predicted values\nactually above true values >= {prob_threshold_x:.1f}'
    ax.text(prob_threshold_x * 1.025, 75, annotation, ha='left', va='center', color='r')

    ax.set_title('Percentage of actual values above threshold if predicted value above threshold')
    ax.set_xlabel('Predicted Value Threshold')
    ax.set_ylabel('True Values % Above Threshold')

    return fig


def get_actual_vs_pred_prob_fig(predicted_values, actual_values, x=None):
    actual_vs_pred_prob = []

    if x is None:
        x = np.linspace(0, 10, 101)

    for prediction_threshold in x:
        # what percentage of predicted_values above prediction_threshold had an actual_value above prediction_threshold
        actual_vs_pred_prob.append(100 * ((actual_values > predicted_values * 1.0) & (predicted_values >
                                                                                            prediction_threshold)).mean())
    fig, ax = plt.subplots()  # Create a figure and an axes.

    ax.plot(x, actual_vs_pred_prob)
    ax.set_title('Percentage of (actual values >= predicted values) if predicted value above threshold')
    ax.set_xlabel('Predicted Value Threshold')
    ax.set_ylabel('True Values % Above Predicted Values')

    return fig


def get_med_actual_if_pred_above_thresh_fig(predicted_values, actual_values, x=None):
    y = []
    if x is None:
        x = np.linspace(2, 10, 101)
    for prediction_threshold in x:
        y.append(np.median(actual_values[predicted_values >= prediction_threshold]))

    fig, ax = plt.subplots()  # Create a figure and an axes.

    ax.plot(x, y)

    best_pred_thresh = find_x_for_max_difference(x, y)
    ax.axvline(x=best_pred_thresh, color='r', linestyle='-')  # Adding a vertical line
    # Annotating the horizontal line
    annotation = (f'Predicted value >= {best_pred_thresh:.2f}%\nmost likely to underestimate')
    ax.text(best_pred_thresh * 0.975, best_pred_thresh, annotation, ha='right', va='center', color='r')

    ax.set_title('Median of actual values if predicted value above threshold')
    ax.set_xlabel('Predicted Value Threshold')
    ax.set_ylabel('Median of Actual Values')

    return fig


def find_x_for_max_difference(x, y, min_x=0):
    # Calculate the differences
    differences = [y_val - x_val for x_val, y_val in zip(x, y) if x_val > min_x]
    # Find the index of the maximum difference
    max_diff_index = differences.index(max(differences))

    # Return the corresponding value of x
    return x[max_diff_index]


def analyze_signal_for_report():
    pass


def get_feature_names(column_transformer):
    """
    Function to get feature names (assuming OneHotEncoder for categorical features)

    Args:
        column_transformer: transformer used in preprocessing

    Returns:
        names of features
    """
    feature_names = []

    # Loop through each transformer in the ColumnTransformer
    for transformer_in_columns in column_transformer.transformers_:
        transformer_name, transformer, cols = transformer_in_columns

        # Check if the transformer isn't "remainder"
        if transformer_name != "remainder":

            # If it's OneHotEncoder, use feature_names_in_ to get names
            if isinstance(transformer, OneHotEncoder):
                for col, categories in zip(cols, transformer.categories_):
                    feature_names.extend([f'{col}_{cat}' for cat in categories])
            else:
                feature_names.extend(cols)

        else:
            # For 'remainder', add the column names

            feature_names.extend([column_transformer.feature_names_in_[col] for col in cols])

    return feature_names
