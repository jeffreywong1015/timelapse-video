import os
import io
import json
import logging
from datetime import datetime, timedelta
import pytz
import tempfile
import shutil
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from googleapiclient.errors import HttpError
from PIL import Image

# Configuration - Get from environment variables
SCOPES = ['https://www.googleapis.com/auth/drive']
FOLDER_A_ID = os.environ['FOLDER_A_ID']
FOLDER_B_NAME = os.environ.get('FOLDER_B_NAME', '10botics.com')
FOLDER_C_NAME = os.environ.get('FOLDER_C_NAME', 'cam1')
IMAGE_FOLDER_NAME = os.environ.get('IMAGE_FOLDER_NAME', 'image')
TIMELAPSE_FOLDER_NAME = os.environ.get('TIMELAPSE_FOLDER_NAME', 'timelapse')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def authenticate():
    """Authenticate using service account credentials from env"""
    try:
        logger.info("Authenticating to Google Drive...")
        creds_json = json.loads(os.environ['SERVICE_ACCOUNT_JSON'])
        credentials = service_account.Credentials.from_service_account_info(
            creds_json, scopes=SCOPES)
        return build('drive', 'v3', credentials=credentials)
    except Exception as e:
        logger.error(f"Authentication failed: {str(e)}")
        raise

def verify_folder_access(service, folder_id):
    try:
        logger.info(f"Verifying access to folder ID: {folder_id}")
        folder = service.files().get(fileId=folder_id, fields='id, name', supportsAllDrives=True).execute()
        logger.info(f"Folder verified: {folder['name']} (ID: {folder['id']})")
        return True
    except HttpError as e:
        logger.error(f"Failed to access folder {folder_id}: {str(e)}")
        return False

def find_or_create_folder(service, parent_id, folder_name):
    try:
        query = f"'{parent_id}' in parents and name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        results = service.files().list(
            q=query,
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        folders = results.get('files', [])
        if folders:
            logger.info(f"Found folder: {folder_name} (ID: {folders[0]['id']})")
            return folders[0]['id']
        
        logger.info(f"Creating folder: {folder_name} under parent ID: {parent_id}")
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }
        folder = service.files().create(
            body=file_metadata,
            fields='id',
            supportsAllDrives=True
        ).execute()
        logger.info(f"Created folder: {folder_name} (ID: {folder['id']})")
        return folder['id']
    except HttpError as e:
        logger.error(f"Error in find_or_create_folder for {folder_name}: {str(e)}")
        raise

def get_folder_ids(service):
    if not verify_folder_access(service, FOLDER_A_ID):
        raise ValueError(f"Cannot access folder with ID: {FOLDER_A_ID}")
    folder_b_id = find_or_create_folder(service, FOLDER_A_ID, FOLDER_B_NAME)
    folder_c_id = find_or_create_folder(service, folder_b_id, FOLDER_C_NAME)
    image_folder_id = find_or_create_folder(service, folder_c_id, IMAGE_FOLDER_NAME)
    timelapse_folder_id = find_or_create_folder(service, folder_c_id, TIMELAPSE_FOLDER_NAME)
    
    # Create subfolders for hourly, daily, and weekly videos
    hourly_folder_id = find_or_create_folder(service, timelapse_folder_id, 'hourly')
    daily_folder_id = find_or_create_folder(service, timelapse_folder_id, 'daily')
    weekly_folder_id = find_or_create_folder(service, timelapse_folder_id, 'weekly')
    
    return image_folder_id, hourly_folder_id, daily_folder_id, weekly_folder_id

def get_hourly_video_info(now):
    start_time = (now - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    end_time = start_time + timedelta(hours=1)
    video_name = f"timelapse_hour_{end_time.strftime('%Y%m%d_%H')}.gif"
    return start_time, end_time, video_name, 250  # 250ms per frame

def get_daily_video_info(now):
    end_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=1)
    video_name = f"timelapse_day_{end_time.strftime('%Y%m%d')}.gif"
    return start_time, end_time, video_name, 100  # 100ms per frame

def get_weekly_video_info(now):
    weekday = now.weekday()  # 0=Monday, 6=Sunday
    days_to_monday = weekday
    end_time = (now - timedelta(days=days_to_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=7)
    year, week, _ = end_time.isocalendar()
    video_name = f"timelapse_week_{year}{week:02d}.gif"
    return start_time, end_time, video_name, 50  # 50ms per frame

def video_exists(service, folder_id, video_name):
    try:
        query = f"'{folder_id}' in parents and name = '{video_name}' and trashed = false"
        results = service.files().list(
            q=query,
            fields="files(id)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        return bool(results.get('files', []))
    except HttpError as e:
        logger.error(f"Error checking video existence for {video_name}: {str(e)}")
        return False

def download_images(service, image_folder_id, start_time, end_time, temp_dir):
    try:
        start_utc = start_time.astimezone(pytz.utc).isoformat().replace('+00:00', 'Z')
        end_utc = end_time.astimezone(pytz.utc).isoformat().replace('+00:00', 'Z')
        query = f"'{image_folder_id}' in parents and createdTime >= '{start_utc}' and createdTime < '{end_utc}' and (mimeType='image/jpeg' or mimeType='image/png' or mimeType='image/jpg')"
        results = service.files().list(
            q=query,
            orderBy="createdTime",
            fields="nextPageToken, files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        files = results.get('files', [])
        logger.info(f"Found {len(files)} images for period {start_time} to {end_time}")
        image_paths = []
        for idx, file in enumerate(files):
            file_path = os.path.join(temp_dir, f"{idx:04d}.jpg")
            request = service.files().get_media(fileId=file['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            fh.seek(0)
            with open(file_path, 'wb') as f:
                f.write(fh.read())
            if os.path.getsize(file_path) > 0:
                image_paths.append(file_path)
                logger.debug(f"Saved image: {file_path}")
            else:
                logger.warning(f"Empty file downloaded: {file['name']}")
                os.remove(file_path)
        return image_paths
    except HttpError as e:
        logger.error(f"Error downloading images: {str(e)}")
        return []

def create_gif(image_paths, output_gif, frame_duration):
    if not image_paths:
        logger.warning("No images provided for GIF creation")
        return False
    try:
        logger.info(f"Creating GIF with {len(image_paths)} images ({frame_duration}ms per frame)")
        
        # Open all images
        images = []
        for path in image_paths:
            try:
                with Image.open(path) as img:
                    images.append(img.copy())
            except Exception as e:
                logger.warning(f"Error processing {path}: {str(e)}")
        
        if not images:
            logger.error("No valid images to create GIF")
            return False
            
        # Save as animated GIF
        images[0].save(
            output_gif,
            save_all=True,
            append_images=images[1:],
            duration=frame_duration,
            loop=0,
            optimize=True
        )
        logger.info(f"Created GIF: {output_gif} ({os.path.getsize(output_gif)/1024:.1f} KB)")
        return True
    except Exception as e:
        logger.error(f"Error creating GIF: {str(e)}")
        return False

def upload_video(service, folder_id, video_path, video_name):
    try:
        file_metadata = {'name': video_name, 'parents': [folder_id]}
        media = MediaFileUpload(video_path, mimetype='image/gif')
        service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id',
            supportsAllDrives=True
        ).execute()
        logger.info(f"Uploaded video: {video_name} to folder ID: {folder_id}")
    except HttpError as e:
        logger.error(f"Error uploading video {video_name}: {str(e)}")
        raise

def delete_videos_in_folder(service, folder_id):
    try:
        query = f"'{folder_id}' in parents and (mimeType='image/gif' or mimeType='video/mp4') and trashed = false"
        results = service.files().list(
            q=query,
            fields="files(id)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        files = results.get('files', [])
        for file in files:
            service.files().delete(fileId=file['id'], supportsAllDrives=True).execute()
            logger.info(f"Deleted video: {file['id']} from folder ID: {folder_id}")
        logger.info(f"Deleted {len(files)} videos from folder ID: {folder_id}")
    except HttpError as e:
        logger.error(f"Error deleting videos from folder ID: {folder_id}: {str(e)}")

def delete_old_images(service, image_folder_id, end_time):
    try:
        end_utc = end_time.astimezone(pytz.utc).isoformat().replace('+00:00', 'Z')
        query = f"'{image_folder_id}' in parents and createdTime < '{end_utc}'"
        results = service.files().list(
            q=query,
            fields="nextPageToken, files(id)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        files = results.get('files', [])
        for file in files:
            service.files().delete(fileId=file['id'], supportsAllDrives=True).execute()
            logger.info(f"Deleted image: {file['id']}")
        logger.info(f"Deleted {len(files)} images before {end_time}")
    except HttpError as e:
        logger.error(f"Error deleting images: {str(e)}")

def process_video_type(service, video_type, get_info_func, image_folder_id, subfolder_ids):
    """Process a single video type (hourly/daily/weekly)"""
    try:
        hkt = pytz.timezone('Asia/Hong_Kong')
        now = datetime.now(hkt)
        
        # Get video info
        start_time, end_time, video_name, frame_duration = get_info_func(now)
        folder_id = subfolder_ids[video_type]
        
        # Skip if video exists
        if video_exists(service, folder_id, video_name):
            logger.info(f"{video_type} video {video_name} already exists, skipping")
            return True
            
        # Create temp directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Download images
            image_paths = download_images(service, image_folder_id, start_time, end_time, temp_dir)
            if not image_paths:
                logger.warning(f"No images found for {video_type} video {video_name}")
                return False
                
            # Create GIF
            output_path = os.path.join(temp_dir, video_name)
            if not create_gif(image_paths, output_path, frame_duration):
                logger.error(f"Failed to create {video_type} video {video_name}")
                return False
                
            # Upload to Drive
            upload_video(service, folder_id, output_path, video_name)
            
            # Cleanup operations
            if video_type == 'daily':
                delete_videos_in_folder(service, subfolder_ids['hourly'])
            elif video_type == 'weekly':
                delete_videos_in_folder(service, subfolder_ids['daily'])
                delete_old_images(service, image_folder_id, end_time)
                
        return True
        
    except Exception as e:
        logger.error(f"Error processing {video_type} video: {str(e)}")
        return False

def main():
    logger.info("===== Google Drive Timelapse Creator =====")
    try:
        service = authenticate()
        image_folder_id, hourly_folder_id, daily_folder_id, weekly_folder_id = get_folder_ids(service)
        
        # Map of video types to their functions and folders
        subfolder_ids = {
            'hourly': hourly_folder_id,
            'daily': daily_folder_id,
            'weekly': weekly_folder_id
        }

        video_processors = [
            ('hourly', get_hourly_video_info),
            ('daily', get_daily_video_info),
            ('weekly', get_weekly_video_info)
        ]

        # Process each video type
        for video_type, get_info in video_processors:
            logger.info(f"Processing {video_type} video...")
            success = process_video_type(service, video_type, get_info, image_folder_id, subfolder_ids)
            logger.info(f"{video_type} processing {'succeeded' if success else 'failed'}")

    except Exception as e:
        logger.error(f"Critical error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()