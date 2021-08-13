#performing important imports
from flask import Flask, request, render_template, send_file, redirect, url_for
from predictionValidation import PredictionValidation
from prediction_data_validation.prediction_data_validation import PredictionDataValidation
from model_prediction.Prediction import Prediction
import os
from application_logging.logger import App_Logger
import pandas as pd
from flask_cors import cross_origin

from werkzeug.utils import secure_filename
app = Flask(__name__)
logger = App_Logger()
ALLOWED_EXTENSIONS = {'csv'}

@app.route("/", methods=["GET"])
@cross_origin()
def home():
    """
    This function initiates the home page
    :return: html
    """
    file_object = open("Prediction_Log/apiHandlerLog.txt", 'a+')
    logger.log(file_object, 'Initiating app', 'Info')
    is_uploaded = False
    try:
        pred_data_val = PredictionDataValidation()
        #deleting Prediction_Files folder
        if os.path.isdir('Prediction_Files/'):
            pred_data_val.deletePredictionFiles()
        # creating Prediction_Files folder
        pred_data_val.createPredictionFiles('Prediction_Files')
        #pred_data_val.createPredictionFiles('Prediction_Log')
        column_info = pred_data_val.getSchemaValues()
        columns = column_info[1]
        educations = columns['EDUCATION'].keys()
        print(educations)
        pays = columns['PAY'].keys()
        logger.log(file_object, 'Deletion and creation of Prediction_Files complete. Exiting method...', 'Info')
        file_object.close()
        return render_template('index.html', data= {'test':"test", 'educations':educations, 'pays':pays})
    except Exception as e:
        logger.log(file_object, f'Exception occured in initating or creation/deletion of Prediction_Files directory. Message: {str(e)}', 'Error')
        file_object.close()
        message = 'Error :: ' + str(e)
        return render_template('exception.html', exception=message)

def allowed_file(filename):
    return '.' in filename and filename.split('.')[1].lower() in ALLOWED_EXTENSIONS

@app.route('/uploader', methods=['GET','POST'])
@cross_origin()
def uploadFiles():
    """
    This function helps to upload the file for prediction
    :return: html
    """
    file_object = open("Prediction_Log/apiHandlerLog.txt", 'a+')
    logger.log(file_object, 'File upload initaited..', 'Info')
    try:
        if os.path.exists('Prediction_Files/Prediction.csv') or len(os.listdir('Prediction_Files/')):
            return redirect(url_for('home'))

        is_uploaded = False
        #getting uploaded file
        if request.method == 'POST':
            if request.files['file'] is not None:
                file = request.files['file']
                #checking if correct file
                if allowed_file(file.filename):
                    file.save(os.path.join('Prediction_Files/',secure_filename(file.filename)))
                    is_uploaded = True
                    logger.log(file_object, 'File upload complete..', 'Info')
                else:
                   return redirect(url_for('home'))


        file_object.close()
        return render_template('index.html', data = {'is_uploaded':is_uploaded})
    except Exception as e:
        logger.log(file_object, f'Error occured in Uploading file. Message: {str(e)}', 'Error')
        file_object.close()
        message = 'Error :: ' + str(e)
        return render_template('exception.html', exception=message)

@app.route('/input', methods=['POST'])
@cross_origin()
def manualInput():
    """
    This function helps to get all the manual input provided by the user
    :return: html
    """
    file_object = open("Prediction_Log/apiHandlerLog.txt", 'a+')
    logger.log(file_object, 'Getting input from Form', 'Info')
    try:
        is_uploaded = True
        #getting data
        if request.method == 'POST':
            input = []
            pred_data_val = PredictionDataValidation()
            requiredColumns = pred_data_val.getSchemaValues()[2]
            columns = pred_data_val.getSchemaValues()[1]
            selected = request.form.to_dict(flat=False)
            print(selected)
            print('a')
            for i, v in enumerate(selected.keys()):
                print('b')
                if v in columns.keys():
                    property = columns[v][selected[v][0]]
                    input.append(property)
                elif v[:-2] == 'PAY':
                    property = columns['PAY'][selected[v][0]]
                    input.append(property)
                else:
                    input.append(selected[v][0])
            print(input)
            pd.DataFrame([input],columns=requiredColumns).to_csv('Prediction_Files/input.csv', index=None)
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
    try:
        if os.path.exists('Prediction_Files/Prediction.csv'):
            return redirect(url_for('home'))
        file_object = open("Prediction_Log/apiHandlerLog.txt", 'a+')
        logger.log(file_object, 'Prediction Initiated..', 'Info')
        pred_val = PredictionValidation()
        #initiating validstion
        pred_val.validation()

        pred = Prediction()
        #calling perdict to perform prediction
        prediction, columns = pred.predict()
        logger.log(file_object, 'Prediction for data complete', 'Info')
        file_object.close()
        return render_template('result.html', result = [enumerate(prediction), columns])
        #return send_file(os.path.join('Prediction_Files/')+'Prediction.csv', as_attachment=True)
    except Exception as e:
        logger.log(file_object, f'Error occured in prediction. Message: {str(e)}', 'Error')
        file_object.close()
        message = 'Error :: '+str(e)
        return render_template('exception.html', exception = message)

@app.route('/download',methods=['GET'])
@cross_origin()
def download():
    """
    This function helps to download the predicted output
    :return: Prediction.csv
    """
    try:
        return send_file(os.path.join('Prediction_Files/')+'Prediction.csv', as_attachment=True)
    except Exception as e:
        message = 'Error :: ' + str(e)
        return render_template('exception.html', exception=message)
if __name__ == "__main__":
    app.run(debug=True)







