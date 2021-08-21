# performing important imports
import os
import pandas as pd
from flask_cors import cross_origin
from zipfile import ZipFile
from flask import Flask, request, render_template, send_file, redirect, url_for
from prediction_validation import PredictionValidation
from prediction_data_validation.prediction_data_validation import PredictionDataValidation
from model_prediction.Prediction import Prediction
from application_logging.logger import AppLogger

app = Flask(__name__)
logger = AppLogger()


@app.route("/", methods=["GET"])
@cross_origin()
def home():
    """
    This function initiates the home page
    :return: html
    """
    file_object = open("prediction_log/apiHandlerLog.txt", 'a+')
    logger.log(file_object, 'Initiating app', 'Info')
    try:
        pred_data_val = PredictionDataValidation()
        # deleting Prediction_Files folder
        if os.path.isdir('Prediction_Files/'):
            pred_data_val.delete_prediction_files()
        # creating Prediction_Files folder
        pred_data_val.create_prediction_files('Prediction_Files')
        column_info = pred_data_val.get_schema_values()
        columns = column_info[1]
        educations = columns['EDUCATION'].keys()
        print(educations)
        pays = columns['PAY'].keys()
        logger.log(file_object, 'Deletion and creation of Prediction_Files complete. Exiting method...', 'Info')
        file_object.close()
        return render_template('index.html', data={'educations': educations, 'pays': pays})
    except Exception as e:
        logger.log(
            file_object,
            f'Exception occured in initating or creation/deletion of Prediction_Files directory. Message: {str(e)}',
            'Error')
        file_object.close()
        message = 'Error :: ' + str(e)
        return render_template('exception.html', exception=message)


@app.route('/input', methods=['POST'])
@cross_origin()
def manual_input():
    """
    This function helps to get all the manual input provided by the user
    :return: html
    """
    file_object = open("prediction_log/apiHandlerLog.txt", 'a+')
    logger.log(file_object, 'Getting input from Form', 'Info')
    try:
        # getting data
        if request.method == 'POST':
            input_data = []
            pred_data_val = PredictionDataValidation()
            required_columns = pred_data_val.get_schema_values()[2]
            columns = pred_data_val.get_schema_values()[1]
            selected = request.form.to_dict(flat=False)
            for i, v in enumerate(selected.keys()):
                print('b')
                if v in columns.keys():
                    property_col = columns[v][selected[v][0]]
                    input_data.append(property_col)
                elif v[:-2] == 'PAY':
                    property_col = columns['PAY'][selected[v][0]]
                    input_data.append(property_col)
                else:
                    input_data.append(selected[v][0])
            print(len(input_data),len(required_columns))
            pd.DataFrame([input_data], columns=required_columns).to_csv('Prediction_Files/input.csv', index=False)
        return redirect(url_for('predict'))
    except Exception as e:
        logger.log(file_object, f'Error occured in getting input from Form. Message: {str(e)}', 'Error')
        file_object.close()
        message = 'Error :: ' + str(e)
        return render_template('exception.html', exception=message)


@app.route('/predict', methods=['GET'])
@cross_origin()
def predict():
    """
    This function is the gateway for data prediction
    :return: html
    """
    file_object = open("prediction_log/apiHandlerLog.txt", 'a+')
    try:
        if os.path.exists('Prediction_Files/Prediction.csv'):
            return redirect(url_for('home'))
        logger.log(file_object, 'Prediction Initiated..', 'Info')
        pred_val = PredictionValidation()
        # initiating validstion
        pred_val.validation()
        pred = Prediction()
        # calling perdict to perform prediction
        output, probability = pred.predict()
        logger.log(file_object, 'Prediction for data complete', 'Info')
        file_object.close()
        return render_template('result.html', result={"output": output, "probability": probability})
        # return send_file(os.path.join('Prediction_Files/')+'Prediction.csv', as_attachment=True)
    except Exception as e:
        logger.log(file_object, f'Error occured in prediction. Message: {str(e)}', 'Error')
        file_object.close()
        message = 'Error :: '+str(e)
        return render_template('exception.html', exception=message)


@app.route('/getLogs', methods=['GET'])
@cross_origin()
def get_logs():
    """
    Returns logs for inspection of the system
    :return: ZIP of log
    """
    try:
        log_files = os.listdir('prediction_log/')
        with ZipFile("Prediction_Files/Logs.zip", "w") as newzip:
            for i in log_files:
                newzip.write("prediction_log/"+i)
        return send_file(os.path.join('Prediction_Files/')+'Logs.zip', as_attachment=True)
    except Exception as e:
        message = 'Error :: ' + str(e)
        return render_template('exception.html', exception=message)


if __name__ == "__main__":
    app.run(debug=True)
