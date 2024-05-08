import paramiko
import os
import yaml

def download_txt_files(host, port, username, password, remote_path, local_path):
    # Initialize SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Connect to the server
        ssh.connect(host, port=port, username=username, password=password)
       
        # Initialize SFTP session
        sftp = ssh.open_sftp()
       
        # Change to the desired directory
        sftp.chdir(remote_path)
       
        # List files in the directory
        files = sftp.listdir()

        # Download each .txt file
        for file in files:
            if file.endswith('.txt'):
                local_filepath = os.path.join(local_path, file)
                sftp.get(file, local_filepath)
                print(f"Downloaded {file} to {local_filepath}")
                sftp.remove(file)
                print(f"Deleted {file} From {remote_path}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close connections
        sftp.close()
        ssh.close()

# Usage
with open(r'/home/jwang/Credentials/CP.yaml') as file:
    ftploginlist = yaml.load(file, Loader=yaml.FullLoader)
    password = ftploginlist["password"]
    Host = ftploginlist["host"]
    username = ftploginlist["username"]
    port = ftploginlist["port"]
remote_dir = '/APT/'
LocalLanding_dir = '/elt/data_bucket/Landing/CP_Status/'
local_dir = '/elt/data_bucket/CP_Status/'
    

download_txt_files(Host, port, username, password, remote_dir, LocalLanding_dir)
