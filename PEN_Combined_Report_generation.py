import pandas as pd


def get_last_timestamp_value(df):
    # Sort the DataFrame by 'sent_on' column in descending order
    df_sorted = df.sort_values(by='sent_on', ascending=False)
    
    # Drop duplicates based on 'phone_number' and 'Amount' columns
    df_unique = df_sorted.drop_duplicates(subset=['phone_number', 'Payment Amount'], keep='first')
    
    return df_unique

def get_clicked_values(df):
    # Create an empty dictionary to store the mapped data
    data_dict = {}

    # Iterate over the rows of the DataFrame
    for index, row in df.iterrows():
        # Extract the key from the combination of 'phone_number' and 'URL'
        key = (row['phone_number'], row['URL'])
        
        # Extract the 'Amount' and 'Status' values
        amount = row['Amount']
        status = 1 if row['Status'] in ['clicked', 'payment_successful'] else 0
        is_payment_success = 'Success' if row['Status'] == 'payment_successful' else '#N/A'
        is_payment_fail = 'Faild' if row['Status'] == 'payment_failed' else '#N/A'
        
        # Check if the key already exists in the dictionary
        if key in data_dict:
            # Update the 'Status' value by adding 1 and 'payment_success' as well as 'payment_failed' values
            data_dict[key][1] += status
            data_dict[key][2] = is_payment_success
            data_dict[key][3] = is_payment_fail
        else:
            # Create a new list with 'Amount', initial 'Status' value, and 'payment' values
            data_dict[key] = [amount, status, is_payment_success, is_payment_fail]

    # Add a new columns 'Clicked', 'payment_success', 'payment_failed' to the DataFrame
    df['Clicked'] = df.apply(lambda row: data_dict.get((row['phone_number'], row['URL']), [-1, -1, -1, -1])[1], axis=1)
    df['payment_success'] = df.apply(lambda row: data_dict.get((row['phone_number'], row['URL']), [-1, -1, -1, -1])[2], axis=1)
    df['payment_failed'] = df.apply(lambda row: data_dict.get((row['phone_number'], row['URL']), [-1, -1, -1, -1])[3], axis=1)

    # Assign '#N/A' to 'Clicked' column when the value from data_dict is 0 (meaning 'Clicked' is 0)
    df.loc[df['Clicked'] == 0, 'Clicked'] = '#N/A'

    # Update 'Payment Amount' column based on 'Clicked' column
    df.loc[df['Clicked'] == '#N/A', 'Payment Amount'] = '#N/A'

    return df


def merge_csv_files(file1, file2, file3, common_field, output_file):
    # Read the CSV files
    dataFrame1 = pd.read_csv(file1)
    dataFrame2 = pd.read_csv(file2)
    dataFrame3 = pd.read_csv(file3)

    # Filter the rows where 'delivery_status' is 'Reached'
    dataFrame1 = dataFrame1[dataFrame1['delivery_status'] == 'Reached']
    dataFrame3 = get_clicked_values(dataFrame3)

    # Merge the DataFrames based on the common fields
    merged_file = pd.merge(dataFrame1, dataFrame2, on=common_field, how='inner')
    merged_file = pd.merge(merged_file, dataFrame3, on=common_field, how='inner')


    # Select the desired columns for the output file
    columns_to_keep = ['phone_number','first_name_x','last_name_x','language_x','requested_on_x','sent_on_x','delivery_status_x',
                       'duration','response_value','tag1','tag2','tag3','tag4','tag5',
                       'delivery_status_y',
                       'Clicked', 'payment_success', 'payment_failed', 'Payment Amount']
    merged_file = merged_file[columns_to_keep]

    # Rename the columns in merged file for better understanding
    merged_file = merged_file.rename(columns={'first_name_x': 'first_name',
                                              'last_name_x': 'last_name',
                                              'language_x': 'language',
                                              'requested_on_x': 'requested_on',
                                              'sent_on_x': 'sent_on',
                                              'delivery_status_x': 'delivery_status',
                                              'delivery_status_y': 'SMS Status'})
    
    # Replace missing values with '#N/A' in the desired columns
    columns_to_replace = ['Payment Amount']
    merged_file[columns_to_replace] = merged_file[columns_to_replace].fillna('#N/A')

    # Drop duplicate rows
    merged_file = get_last_timestamp_value(merged_file)

    # Save the merged DataFrame to a new CSV file
    merged_file.to_csv(output_file, index=False)
    

# Set the file paths and output file name
file1 = 'Attempts-Digital_Loan_Installment_Reminder_2023-05-01_2023-05-31' + '.csv'    # Voice file name
file2 = 'Attempts-Digital_Loan_Installment_Reminder-account_number-SMS_2023-05-01_2023-05-31' + '.csv'    # SMS file name
file3 = 'payment_link_interaction_data_for_ad_f68e95_from_2023-05-12 00_00_00_to_2023-06-02 00_00_00' + '.csv'    # Payment file name
output_file = 'PEN_Combined_Report_File' + '.csv'

common_field = 'phone_number'

# Call the function to merge the CSV files
merge_csv_files(file1, file2, file3, common_field, output_file)