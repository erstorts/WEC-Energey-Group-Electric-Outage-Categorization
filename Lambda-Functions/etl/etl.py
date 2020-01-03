#cleaning data
import pandas as pd
import datetime
import re
import os
import boto3


def clean_comments(text):
    '''
    This function will remove the common text before the comments
    '''
    pattern_1 = r"ON LtOut WE\d{4,5} SAYS\s"
    regex_1 = re.sub(pattern_1, '', text)
    pattern_2 = r"ON LtOut TRBL SAYS"
    regex_2 = re.sub(pattern_2, '', regex_1)
    pattern_3 = r"ON HAZ TRBL SAYS"
    regex_3 = re.sub(pattern_3, '', regex_2)
    new_text = regex_3.replace('@', 'at')
    pattern_4 = r"[^a-zA-Z0-9]+"
    return re.sub(pattern_4, ' ', new_text)
    
    
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
        
def lambda_handler(event, context):
    '''
    This function will import and run data throught the nlp model
    '''
    #set the working directory to where it can read and write
    os.chdir('/tmp/')
    
    #make an s3 object
    s3 = boto3.client('s3')
    #get the bucket from the event object
    bucket = event['Records'][0]['s3']['bucket']['name']
    
    asset_files = s3.list_objects(Bucket=bucket, Prefix='testing/assets/')['Contents']

    for file in asset_files:
        the_file = file['Key']
        file_name = the_file.split('/')[-1]
        s3.download_file(bucket, the_file, file_name)
    
    
    
    #get to day's date
    today = datetime.datetime.today()
    
    #file names in s3
    we_file = 'We-Outages_{}_{}_{}.xlsx'.format(today.year, 
                                                today.month,
                                               today.day)
    
    wps_file = 'WPS-Outages_{}_{}_{}.xlsx'.format(today.year, 
                                                today.month,
                                               today.day)
    
    #list of file names
    all_files = [we_file, wps_file]
    
    
    #copy model results from s3 to instance
    for file in all_files:
        import_location = 'testing/import/{}'.format(file)
        s3.download_file(bucket, import_location, file)
                                                                
    #import data
    we_outages = pd.read_excel(we_file)
    wps_outages = pd.read_excel(wps_file)
    
    
    #save raw data to new s3 location
    we_response = s3.upload_file(we_file,
                                    Bucket=bucket, 
                                    Key='testing/raw/{}'.format(we_file))
    
    wps_response = s3.upload_file(wps_file,
                                    Bucket=bucket, 
                                    Key='testing/raw/{}'.format(wps_file))
                                    
    #columns to keep in each file                               
    we_keep = ['Outage', 'Mobile Data Remarks']
    wps_keep = ['Event', 'ClosureRemarks']
    
    we_save = we_outages[we_keep]
    wps_save = wps_outages[wps_keep]
    
    #adding company so we can work with one dataframe
    we_save['Company'] = 'We Energies'
    wps_save['Company'] = 'WPS'
    
    we_save = we_save.rename(columns={'Outage': 'Outage ID'})
    wps_save = wps_save.rename(columns={'Event': 'Outage ID', 'ClosureRemarks': 'Mobile Data Remarks'})
    
    
    #make one dataframe
    all_outages = pd.concat([we_save, wps_save], sort=True)
    
    #make sure comments are strings
    all_outages['Mobile Data Remarks'] = all_outages['Mobile Data Remarks'].map(str)
    #clean comments
    all_outages['Mobile Data Remarks'] = all_outages['Mobile Data Remarks'].apply(clean_comments)                                    
    
    #drop missings
    all_outages = all_outages.dropna()
    all_outages = all_outages[all_outages['Mobile Data Remarks'] != 'nan']
    all_outages = all_outages[all_outages['Mobile Data Remarks'] != ' ']

    #file name
    model_file_location = ('Model_{}_{}_{}.csv').format(today.year, 
                                                             today.month,
                                                            today.day)
    
    #save to csv
    all_outages.to_csv(model_file_location, index=False)
    
    #svae to s3
    results_response = s3.upload_file(Filename=model_file_location,
                                    Bucket=bucket, 
                                    Key='testing/to_model/{}'.format(model_file_location))
                                    
    return print('Data is Ready for model.')