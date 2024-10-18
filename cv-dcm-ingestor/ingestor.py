import json
from database import dbM
import sys, getopt, os, time
from PIL import Image
import pydicom
from datetime import datetime
from serverapi import DBAPI
import logging.config
from logging.handlers import TimedRotatingFileHandler
from utils import get_files_with_extensions, list_image_files
import pandas as pd

log_filename = "logs/watchman.log"
os.makedirs(os.path.dirname(log_filename), exist_ok=True)
log_handler = TimedRotatingFileHandler(
    log_filename, when="midnight", interval=1, encoding="utf8", backupCount=10
)
log_handler.suffix = "%Y-%m-%d"
formatter = logging.Formatter(
    "%(asctime)-15s" "| %(threadName)-11s" "| %(levelname)-5s" "| %(message)s"
)
log_handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(log_handler)


def load_label_file(file_path):
    if os.path.exists(file_path):
        if file_path.endswith(".json"):
            with open(file_path, "r") as f:
                data = json.load(f)
                data_items = list(data.items())
                # Create a DataFrame from the items
                df = pd.DataFrame(data_items, columns=["imageID", "Labels"])
        elif file_path.endswith(".csv"):
            df = pd.read_csv(label_file)
        else:
            print(
                "Unknown file type. please make sure the labels file is either csv or json."
            )
            exit(1)
    else:
        print("label file missing")
        exit(1)
    return df


def get_label(imageID):
    row = df.loc[df["image_name"] == imageID]
    print("labels of received image: ", row)
    logger.info(f"labels of received image: {row}")
    return row


def get_images_count():
    cursor = dbM.cursor(buffered=True)
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    result = cursor.fetchone()
    return result[0]


def insert2db(
    imageID,
    label,
    company,
    annotation,
    image_status,
    image_intent,
    created_date,
    height,
    width,
):
    # once all metadata of image is extracted save it into raw table
    cursor = dbM.cursor(buffered=True)
    try:
        cursor.execute(
            f"SELECT * FROM {table_name} WHERE imageId='{imageID}' and annotation='{annotation}'"
        )
        result = cursor.fetchone()
        print("found result", result)

        # if image is not inserted into autonomous_raw_data table
        if result is None:
            sql = f"""INSERT INTO {table_name} (
                imageId, label, company, image_intent, annotation, image_status, 
                created_date, updated_date,
                height, width
                ) VALUES    (
                    %s, %s, %s, %s, %s, %s, 
                    %s, %s, 
                    %s, %s
                ) """
            val = (
                imageID,
                label,
                company,
                image_intent,
                annotation,
                image_status,
                created_date,
                created_date,
                height,
                width,
            )
            print("sql", sql)
            print("\n\nval", val)

            cursor.execute(sql, val)
            dbM.commit()
            # data for server
            data = {
                "data_id": imageID,
                "company": company,
                "label": label,
                "annotation": "",
                "data_intent": image_intent,
                "is_sample": False,
                "is_active": image_status,
                "image_height": height,
                "image_width": width,
            }
            # send data to server
            serverAPI_obj.sendMetaData(data)
    except Exception as err:
        print(f"Error in inser2db as {err} at {sys.exc_info()[-1].tb_lineno}")
    cursor.close()


def convert_dcm_file(input_image_path, dest_image_path):
    # Load the .dcm file
    dcm = pydicom.dcmread(input_image_path, force=True)
    # Convert the pixel data to a numpy array
    arr = dcm.pixel_array
    # Convert the numpy array to a PIL image
    img = Image.fromarray(arr)
    # Save the image as a PNG file
    img.save(dest_image_path, "PNG")


def convert_other_file(input_image_path, dest_image_path):
    # Load the image file
    img = Image.open(input_image_path)
    # Save the image as a PNG file
    img.save(dest_image_path, "PNG")


def process_image(file_path):
    try:
        print(f"Someone created {file_path}")
        print("Image id found", file_path)
        logger.info(f"Image id found {file_path}")
        created_date = datetime.now()
        filename = os.path.split(file_path)[1]
        image_id = f"{filename}"

        # get the object/ label of image received
        # in future the function will be provided just path to handle all categories
        label_data = get_label(filename)
        print("label_data", label_data)
        if label_data is None:
            raise ModuleNotFoundError
        image_status = 1
        height = 0
        width = 0

        print(f"fileName: {file_path}")
        print(f"image_id: {image_id}")
        print(f"company: {company}")
        print(f"image_status: {image_status}")
        print(f"dateCreated: {created_date}")
        print(f"image_intent: {image_intent}")
        annotations_by_label = {}

        for index, data in label_data.iterrows():
            # Ideally the following data should come from the command line or directly from the Xray Machine
            annotation = (data["x1"], data["y1"], data["x2"], data["y2"])
            label = data["class"]
            height = data["image_height"]
            width = data["image_width"]

            # If the label is not already in the dictionary, add it with an empty list
            if label not in annotations_by_label:
                annotations_by_label[label] = []

            # Append the annotation to the appropriate list
            annotations_by_label[label].append(annotation)

        # Now you can iterate over the dictionary and process grouped annotations
        for label, annotations in annotations_by_label.items():
            print(f"label: {label}")
            print(f"annotations: {annotations}")

            try:
                insert2db(
                    image_id,
                    label,
                    company,
                    str(annotations),
                    image_status,
                    image_intent,
                    created_date,
                    height,
                    width,
                )
            except Exception as err:
                print(f"Error in insertion as {err} at {sys.exc_info()[-1].tb_lineno}")
    except Exception as e:
        print("error process_image: ", e)
        raise e


if __name__ == "__main__":
    argv = sys.argv[1:]

    # default path where raw images are copied by Watchman M
    src_path = "Machine"
    final_path = "CancerImages"
    label_file = "label.csv"
    userName = "testedge"
    passwd = "&6edg*D9e"
    env = "dev"
    company = "SKU"
    table_name = "sku_data"
    image_intent = "train"

    options, args = getopt.getopt(
        argv,
        "i:o:l:c:d:t:u:p:e:",
        [
            "src_path=",
            "final_path=",
            "label_file=",
            "company=",
            "table_name=",
            "image_intent=" "userName=",
            "passwd=",
            "env=",
        ],
    )
    print("OPTIONS   :", options)

    for opt, arg in options:
        if opt in ("-i", "--src_path"):
            src_path = arg
        elif opt in ("-o", "--final_path"):
            final_path = arg
        elif opt in ("-l", "--label_file"):
            label_file = arg
        elif opt in ("-c", "--company"):
            company = arg
        elif opt in ("-d", "--table_name"):
            table_name = arg
        elif opt in ("-t", "--image_intent"):
            image_intent = arg
        elif opt in ("-u", "--username"):
            userName = arg
        elif opt in ("-p", "--password"):
            passwd = arg
        elif opt in ("-e", "--env"):
            env = arg

    if os.environ["EDGE_USERNAME"]:
        src_path = os.environ["SRC_PATH"]
        final_path = os.environ["DEST_PATH"]
        label_file = os.environ["LABEL_FILE"]
        company = os.environ["COMPANY"]
        table_name = os.environ["TABLE_NAME"]
        image_intent = os.environ["IMAGE_INTENT"]
        userName = os.environ["EDGE_USERNAME"]
        passwd = os.environ["EDGE_PASSWORD"]
        env = os.environ["EDGE_ENV"]

    if not os.path.exists(src_path):
        print(('Input path "%s" does not exist') % (src_path))
        sys.exit()

    if not os.path.exists(final_path):
        os.mkdir(final_path)

    try:
        serverAPI_obj = DBAPI(userName=userName, passwd=passwd, env=env)
    except:
        pass

    df = load_label_file(label_file)

    print(f"connection successfull {dbM} ")

    extensions = ["*.jpg", "*.jpeg", "*.png", "*.bmp", "*.dcm"]
    start_time = time.time()  # Start timing

    found_files = list_image_files(src_path, extensions)
    print("found_files", found_files)
    if len(found_files) == int(get_images_count()):
        exit(1)
    files_not_processed = []
    try:
        for file in found_files:
            try:
                process_image(file)
            except Exception as e:
                print("error process_image(): ", e)
                print("file", file)
                files_not_processed.append(file)
        print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
        print("All files processed successfully")
        end_time = time.time()  # End timing
        elapsed_time = end_time - start_time
        minutes = elapsed_time // 60
        seconds = elapsed_time % 60
        print(f"Time taken: {int(minutes)} minutes and {seconds:.2f} seconds")
        print("files that are not processed: ", files_not_processed)
        print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")

    except KeyboardInterrupt:
        pass
