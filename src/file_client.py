import os

class FileClient:
    @staticmethod
    def delete_file(file_path: str) -> bool:
        try:
            os.remove(file_path)
            print(f"file successfully deleted: {file_path}")
            return True
        except Exception as e:
            print(f"error deleting file: {e}")
            return False
        
    @staticmethod
    def delete_all_files_in_folder(folder_path: str) -> bool:
        try:
            for filename in os.listdir(folder_path):
                file_path = os.path.join(folder_path, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            print(f"files successfully deleted")
            return True
        except Exception as e:
            print(f"error deleting files: {e}")
            return False
