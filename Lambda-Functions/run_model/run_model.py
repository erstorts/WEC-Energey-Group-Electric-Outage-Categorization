#cleaning data
import pandas as pd
import datetime
import os
import boto3
#prep for model
from sklearn.feature_extraction.text import CountVectorizer
#pulling in the model
from sklearn.externals import joblib

    
def category_name(data, decoder_obj):
    '''
    This function takes model predictions and returns the word category
    '''
    result = []
    for pred in data:
        result.append(decoder_obj[pred])
    return result
    
def get_decoder(general_category):

    decoder_list = []
    f = open(general_category + '.txt', 'r')
    f1 = f.readlines()
    for x in f1:
        x = x[:-1]
        decoder_list.append(x)

    decoder_dict = {}
    for i in range(0, len(decoder_list)):
        decoder_dict[i] = decoder_list[i]
        
    return decoder_dict
    
def predict_category(data, category):
    if category == 'Planned':
        data['Subcategory Predictions'] = 'New Construction'
        return data
    elif category == 'Power-Supply':
        data['Subcategory Predictions'] = 'Transmission'
        return data
    else:
        if category == 'General':
            var_name = 'General'
        else:
            var_name = 'Subcategory'
        
        
        #import text vectorizer
        count_vectorizer = joblib.load(category + '-count-vectorizer.pkl')
        #import model
        model = joblib.load(category +'-model.pkl')  
        
        if category != 'General':
        #limit the data
            data = data[data['General Predictions'] == str(category)]
            
        #vectorize text
        X = count_vectorizer.transform(data['Mobile Data Remarks'])

        # Use the loaded model to make predictions
        try:
            pred = model.predict(X)
            pred_prob = model.predict_proba(X)
            
            confidence = []
            for i in range(0,len(pred_prob)):
                confidence.append(pred_prob[i][pred[i]])
        except:
            data[var_name + ' Predictions'] = ''
            data[var_name + ' Confidence'] = ''
            return data

        decoder = get_decoder(category)
        

        data[var_name + ' Predictions'] = category_name(pred, decoder)
        data[var_name + ' Confidence'] = confidence

        return data
        
def lambda_handler(event, context):
    '''
    This function will import and run data throught the nlp model
    '''
    #set the working directory to where it can read and write
    os.chdir('/tmp/')
    
    #make an s3 object
    s3 = boto3.client('s3')
    #get the bucket from the event object
    bucket = 'distribution-reliability-nlp' #event['Records'][0]['s3']['bucket']['name']
    
    asset_files = s3.list_objects(Bucket=bucket, Prefix='testing/assets/')['Contents']

    for file in asset_files:
        the_file = file['Key']
        file_name = the_file.split('/')[-1]
        s3.download_file(bucket, the_file, file_name)
    
    
    
    #get to day's date
    today = datetime.datetime.today()
    
    #file names in s3
    model_file = 'Model_{}_{}_{}.csv'.format(today.year, 
                                            today.month,
                                             today.day)

    
    import_location = 'testing/to_model/{}'.format(model_file)
    s3.download_file(bucket, import_location, model_file)
                                                            
    #import data
    all_outages = pd.read_csv(model_file)

    #predict the general category
    all_outages = predict_category(all_outages, 'General')
    
    #make a list of general categories to use later
    outage_types = ['Equipment', 'Vegetation', 'Public', 'Wildlife', 'Weather', 
                'Other', 'Power-Supply', 'Planned']
    
    #loop through general categories and predict the subcategories
    results_list = []
    for types in outage_types:
        results_list.append(predict_category(all_outages, types))
    
    #combine all subcategory predictions
    results_df = pd.concat(results_list, sort=True)
    
    #limit the dataframe to what is needed
    results_df = results_df[['Outage ID', 'Company', 
                            'General Predictions', 'General Confidence', 
                            'Subcategory Predictions', 'Subcategory Confidence']]
    
    #add columns for human review eval
    results_df['Was the Outage Reportable?'] = ''
    results_df['Reportable Reference Outage ID'] = ''
    results_df['Did You Change the General Category?'] = ''
    results_df['Was the General Prediction Correct?'] = ''
    results_df['Did You Change the Subcategory Category?'] = ''
    results_df['Was the Subcategory Prediction Correct?'] = ''    
    
    #file name
    results_file_location = ('NLP-Results_{}_{}_{}.csv').format(today.year, 
                                                             today.month,
                                                            today.day)
    
    #save to csv
    results_df.to_csv(results_file_location, index=False)
    
    #svae to s3
    results_response = s3.upload_file(Filename=results_file_location,
                                    Bucket=bucket, 
                                    Key='testing/export/{}'.format(results_file_location))
                                    
    return print('Predictions Made.')