import pandas as pd
import os


class CSVMerger:
    def __init__(self, file1, file2, file3, file4_list, output_file):
        """
        Initializes the CSVMerger class.

        Args:
            file1 (str): Path to the Voice file.
            file2 (str): Path to the SMS file.
            file3 (str): Path to the Payment file.
            file4 (str): Path to the Import Summary file.
            output_file (str): Name of the output file.
        """
        self.file1 = file1
        self.file2 = file2
        self.file3 = file3
        self.file4_list = file4_list
        self.output_file = output_file

        # Common fields to merge the DataFrames on
        self.common_field = ['Phone Number', 'phone_number', 'msg_id_y', 'id']
        self.columns_to_replace = ['Payment Amount']

        # Columns to keep from each DataFrame
        self.columns_to_keep_part1 = ['phone_number_x', 'name', 'language_x', 'requested_on_x', 'sent_on_x', 'delivery_status_x',
                                      'duration', 'response_value']
        self.tag_fields = []
        self.columns_to_keep_part2 = ['delivery_status_y', 'Clicked', 'payment_success', 'payment_failed', 'Payment Amount']

        # Select the desired columns for the output file
        # part1 + tag_fields + part2
        self.columns_to_keep = []

        # Rename the columns in the merged file for better understanding
        self.rename_columns = {'phone_number_x': 'phone_number',
                               'language_x': 'language',
                               'requested_on_x': 'requested_on',
                               'sent_on_x': 'sent_on',
                               'delivery_status_x': 'delivery_status',
                               'delivery_status_y': 'SMS Status'}

    def get_last_timestamp_value(self, df):
        """
        Returns a DataFrame with only the last occurrence of each unique 'phone_number' and 'Payment Amount' combination,
        sorted by the 'sent_on' column in descending order.
        """

        # Sort the DataFrame by 'sent_on' column in descending order
        df_sorted = df.sort_values(by='sent_on', ascending=False)

        # Drop duplicates based on 'phone_number' and 'Payment Amount' columns
        df_unique = df_sorted.drop_duplicates(
            subset=['phone_number', 'Payment Amount'], keep='first')

        return df_unique

    def get_clicked_values(self, df):
        """
        Processes the Payment file DataFrame to calculate the 'Clicked', 'payment_success', and 'payment_failed' columns.

        Args:
            df (pd.DataFrame): Payment file DataFrame.

        Returns:
            pd.DataFrame: Processed Payment file DataFrame.
        """

        # Create an empty dictionary to store the mapped data
        data_dict = {}

        # Iterate over the rows of the DataFrame
        for index, row in df.iterrows():
            # Extract the key from the combination of 'phone_number' and 'URL'
            key = (row['Phone Number'], row['URL'])

            # Extract the 'Amount' and 'Status' values
            amount = row['Amount']
            status = 1 if row['Status'] in [
                'clicked', 'payment_successful'] else 0
            is_payment_success = 'Success' if row['Status'] == 'payment_successful' else '#N/A'
            is_payment_fail = 'Failed' if row['Status'] == 'payment_failed' else '#N/A'

            # Check if the key already exists in the dictionary
            if key in data_dict:
                # Update the 'Status' value by adding 1 and 'payment_success' as well as 'payment_failed' values
                data_dict[key][1] += status
                data_dict[key][2] = is_payment_success
                data_dict[key][3] = is_payment_fail
            else:
                # Create a new list with 'Amount', initial 'Status' value, and 'payment' values
                data_dict[key] = [amount, status,
                                  is_payment_success, is_payment_fail]

        # Add new columns 'Clicked', 'payment_success', 'payment_failed' to the DataFrame
        df['Clicked'] = df.apply(lambda row: data_dict.get(
            (row['Phone Number'], row['URL']), [-1, -1, -1, -1])[1], axis=1)
        df['payment_success'] = df.apply(lambda row: data_dict.get(
            (row['Phone Number'], row['URL']), [-1, -1, -1, -1])[2], axis=1)
        df['payment_failed'] = df.apply(lambda row: data_dict.get(
            (row['Phone Number'], row['URL']), [-1, -1, -1, -1])[3], axis=1)

        # Assign '#N/A' to 'Clicked' column when the value from data_dict is 0 (meaning 'Clicked' is 0)
        df.loc[df['Clicked'] == 0, 'Clicked'] = '#N/A'

        # Update 'Payment Amount' column based on 'Clicked' column
        df.loc[df['Clicked'] == '#N/A', 'Payment Amount'] = '#N/A'

        return df

    def read_csv_or_excel_file(self, file_path):
        # Check the file extension of file and read accordingly
        file_extension = os.path.splitext(file_path)[-1].lower()
        if file_extension == '.csv':
            return pd.read_csv(file_path)
        elif file_extension == '.xlsx':
            return pd.read_excel(file_path)
        else:
            print(
                f"Error: Invalid file format for {file_path}. Only CSV and XLSX formats are supported.")
            return None

    def merge_csv_files(self):
        """
        Merges the CSV files based on common fields and performs necessary
        transformations on the merged DataFrame.
        Saves the merged DataFrame to a new CSV file.
        """

        # Read the CSV files
        try:
            dataFrame1 = self.read_csv_or_excel_file(self.file1)
            dataFrame2 = self.read_csv_or_excel_file(self.file2)
            dataFrame3 = self.read_csv_or_excel_file(self.file3)
            dataFrame4 = pd.DataFrame()

            for file in self.file4_list:
                df = self.read_csv_or_excel_file(file)
                dataFrame4 = pd.concat([dataFrame4, df])
            dataFrame4.drop_duplicates(subset=['phone_number', 'id'], inplace=True)

        except FileNotFoundError as e:
            print(f"Error: {e.filename} not found.")
            return
        except Exception as e:
            print(f"Error: Failed to read files. {e}")
            return

        # Filter the rows where 'delivery_status' is 'Reached'
        dataFrame1 = dataFrame1[dataFrame1['delivery_status'] == 'Reached']
        dataFrame3 = self.get_clicked_values(dataFrame3)

        # Merge the DataFrames based on the common fields
        merged_file = pd.merge(dataFrame1, dataFrame2,
                               on=self.common_field[1], how='inner')
        merged_file = pd.merge(
            merged_file, dataFrame3, left_on=self.common_field[1], right_on=self.common_field[0], how='inner')
        merged_file = pd.merge(
            merged_file, dataFrame4, left_on=self.common_field[2], right_on=self.common_field[3], how='inner')

        # Extract all the tag fields from the voice file 
        # Assuming that the tag fields are same in both the voice and import_summary files
        self.tag_fields = [column for column in merged_file.columns if (column.startswith('tag') and column.endswith('x'))]

        # Rename the tag columns in the merged file 
        self.rename_columns.update({tag_column: tag_column[:-2] for tag_column in self.tag_fields})

        self.columns_to_keep = self.columns_to_keep_part1 + self.tag_fields + self.columns_to_keep_part2
        merged_file = merged_file[self.columns_to_keep]
        merged_file = merged_file.rename(columns=self.rename_columns)

        # Replace missing values with '#N/A' in the desired columns
        merged_file[self.columns_to_replace] = merged_file[self.columns_to_replace].fillna(
            '#N/A')

        # Drop duplicate rows
        merged_file = self.get_last_timestamp_value(merged_file)

        # Save the merged DataFrame to a new CSV file
        merged_file.to_csv(self.output_file, index=False)


if __name__ == "__main__":
    # User inputs for file paths and output file name
    file1 = input("Enter file path for Voice file: ")
    file2 = input("Enter file path for SMS file: ")
    file3 = input("Enter file path for Payment file: ")
    file4_paths = (input("Enter file paths for Import Summary files (comma-separated): "))
    files4 = file4_paths.split(",")
    output_file = "PEN_Combined_Report.csv"

    # Create an instance of the CSVMerger class and call the merge_csv_files method
    csv_merger = CSVMerger(file1, file2, file3, files4, output_file)
    csv_merger.merge_csv_files()
