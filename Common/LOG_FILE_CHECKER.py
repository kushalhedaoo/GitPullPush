from datetime import datetime

class LogFileCheck:
    def __init__(self, gcs_object, logs_folder_name='logs'):
        self.gcs_object = gcs_object
        self.logs_folder_name = logs_folder_name

    def _create_logs_folder(self):
        # Check if the logs folder exists, and create it if not
        if not self.gcs_object.blob(self.logs_folder_name + '/'):
            self.gcs_object.blob(self.logs_folder_name + '/').upload_from_string('')  # Create the folder

    def _get_log_file_content(self):
        log_file_name = f'{self.logs_folder_name}/{datetime.now().strftime("%Y_%m_%d")}_log.txt'
        log_file = self.gcs_object.blob(log_file_name)
        return log_file.download_as_text() if log_file.exists() else ""

    def _upload_log_file(self, content):
        log_file_name = f'{self.logs_folder_name}/{datetime.now().strftime("%Y_%m_%d")}_log.txt'
        log_file = self.gcs_object.blob(log_file_name)
        log_file.upload_from_string(content)

    def log_file_exist(self,task_name, gcs_file_list):
        self._create_logs_folder()
        existing_content = self._get_log_file_content()
        log_file_content = existing_content + ('-'*50+'\n'
                                               f'Task: {task_name}\nStart Time: {datetime.now()}\n'
                                               f'Files being processed: {gcs_file_list}\n\n')
        self._upload_log_file(log_file_content)

    def log_file_doesnot_exist(self,task_name):
        self._create_logs_folder()
        existing_content = self._get_log_file_content()
        log_file_content = existing_content + ('-'*50+'\n'
                                               f'Task: {task_name}\nStart Time: {datetime.now()}\n'
                                               'No files found in the GCS bucket.\n\n')
        self._upload_log_file(log_file_content)

    def log_file_zero_size(self,task_name):
        self._create_logs_folder()
        existing_content = self._get_log_file_content()
        log_file_content = existing_content + ('-'*50+'\n'
                                               f'Task: {task_name}\nStart Time: {datetime.now()}\n'
                                               'Some files found in the GCS bucket have 0 KB size.\n\n')
        self._upload_log_file(log_file_content)

    def log_error(self,task_name,error_message):
        self._create_logs_folder()
        existing_content = self._get_log_file_content()
        log_file_content = existing_content + ('-'*50+'\n'
                                               f'Task: {task_name}\nStart Time: {datetime.now()}\n'
                                               f'{error_message}\n\n')
        self._upload_log_file(log_file_content)