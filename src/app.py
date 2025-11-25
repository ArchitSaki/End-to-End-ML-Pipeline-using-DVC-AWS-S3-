import pandas as pd
import logging
import os
from sklearn.model_selection import train_test_split

log_dir="logs"
os.makedirs(log_dir,exist_ok=True)

logger= logging.getLogger('data_ingestion')
logger.setLevel('Debug')

console_handler = logging.StreamHandler()
console_handler.setLevel('Debug')

log_file_path = os.path.join(log_dir, 'data_ingestion.log')
file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel('Debug')

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

def load_params(data_url):
    try:
        df=pd.read_csv(data_url)
        logger.debug('data loaded from %s',data_url)
        return df
    except pd.errors.ParserError as e:
        logger.error('failed to parse %s',e)
        raise
    except Exception as e:
        logger.error('unexpected error %s',e)
        raise

def preprocess_data (df):
    try:
        df.drop(columns=['Unnamed: 2','Unnamed: 3','Unnamed: 4'],inplace=True)
        df.rename(columns={'v1': 'target','v2':'text'},inplace=True)
        logger.debug('data preprocessing completed')
        return df
    except KeyError as e:
        logger.error('missing column in df %s',e)
        raise
    except Exception as e:
        logger.error('unexpected error during preprocessing %s',e)
        raise

def save_data(train,test,data_path):
    try:
        raw_data_path = os.path.join(data_path,'raw')
        os.makedirs(raw_data_path,exist_ok=True)
        train_data.to_csv(os.path.join(raw_data_path,"train.csv"),index=False)
        tesr_data.to_csv(os.path.join(raw_data_path,"test.csv"),index=False)
        logger.debug('train and test data saved %s',raw_data_path)
    except Exception as e:
        logger.error('unexpected error %s',e)
        raise

def main():
    try:
        test_size=0.2
        data_path=''
        df=load_data(data_path)
        final_df=preprocess_data(df)
        train_data,test_data = train_test_split(final_df,test_size,data_path)
        save_data(train_data,test_data,data_path="./data")
    except Exception as e:
        logger.error('Failed to complete data ingestion %s',e)
        print(f"Error: {e}")

if __name__ == '__main__':
    main()