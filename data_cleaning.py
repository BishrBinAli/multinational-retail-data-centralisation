import pandas as pd
import re
import numpy as np

class DataCleaning:
    def clean_user_data(self, user_df):

        # Removing rows with no user_uuid
        cleaned_user_df = user_df[user_df['user_uuid'] != 'NULL']

        # Converting date columns to datetime
        cleaned_user_df['date_of_birth'] = pd.to_datetime(
            cleaned_user_df['date_of_birth'], infer_datetime_format=True, errors='coerce')
        cleaned_user_df['join_date'] = pd.to_datetime(
            cleaned_user_df['join_date'], infer_datetime_format=True, errors='coerce')

        # Removing rows with data that does not make sense
        cleaned_user_df = cleaned_user_df[cleaned_user_df['date_of_birth'].notna(
        )]

        # Removing rows with join date before date of birth
        cleaned_user_df = cleaned_user_df[~(cleaned_user_df['date_of_birth'] > cleaned_user_df['join_date'])]

        # Converting country and country code columns to category type
        cleaned_user_df['country'] = cleaned_user_df['country'].astype('category')
        cleaned_user_df['country_code'] = cleaned_user_df['country_code'].astype('category')
        
        # Setting country code values according to country
        country_code_dict = {
            'United Kingdom' : 'GB',
            'United States' : 'US',
            'Germany' : 'DE'
        }
        cleaned_user_df['country_code'] = cleaned_user_df['country'].map(country_code_dict)

        # Making all the phone numbers to a single format and
        # seperating extensions to another column
        def format_phone_num(x):
            phone_num = x['phone_number']
            country = x['country']
            extension = ''
            extension_index = phone_num.find('x')
            if extension_index != -1:
                extension = phone_num[extension_index+1:]
                phone_num = phone_num[:extension_index]
            else:
                extension = np.nan

            phone_num = re.sub('\D',"", phone_num)
            phone_num = phone_num[-10:]
            dial_code_dict = {
                'United States': '+1',
                'United Kingdom': '+44',
                'Germany': '+49'
            }
            phone_num = dial_code_dict[country] + phone_num

            return [phone_num, extension]

        cleaned_user_df[['phone_number', 'phone_extension']] = cleaned_user_df.apply(format_phone_num, axis=1, result_type='expand')
        
        return cleaned_user_df
