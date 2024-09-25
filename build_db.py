import boto3
import psycopg2
import psycopg2.extras
import toml
import uuid
import main
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from tqdm import tqdm

class pgObj():
    def __init__(self, apod_json):
        try:
            self.date = apod_json['date']
            self.title = apod_json['title']
            self.explanation = apod_json['explanation']
            self.url = apod_json['url']
            self.hdurl = apod_json['hdurl']
            self.media_type = apod_json['media_type']
        except Exception as e:
            pass


    def connect_to_db(self, db_name, db_user, db_password, db_port):
        psycopg2.extras.register_uuid()
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            port=db_port)
        conn.autocommit = True
        cur = conn.cursor()
        return conn, cur


    def write_to_db(self, db_name, db_user, db_password, db_port, table_name):
        result = None
        psycopg2.extras.register_uuid()
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            port=db_port)
        conn.autocommit = True
        cur = conn.cursor()
        obj_uuid = uuid.uuid4()
        try:
            cur.execute(f"""INSERT INTO {table_name} (title, date, explanation, media_type, apod_url, apod_hdurl, id) VALUES (%s, %s, %s, %s, %s, %s, %s)""", (self.title, self.date, self.explanation, self.media_type, self.url, self.hdurl, obj_uuid))
            conn.commit()
            result = 1
        except Exception as e:
            pass
            result = 0
        conn.close()
        return result

class s3Obj():
    def __init__(self, s3_bucket, access_key, secret_key):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    def upload_images(self, db_name, db_user, db_password, db_port, table_name, count):
        s3_results = 0
        psycopg2.extras.register_uuid()
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            port=db_port)
        conn.autocommit = True
        cur = conn.cursor()
        # get all images that have not been uploaded to s3
        cur.execute(f"SELECT * FROM {table_name} WHERE in_s3 = {False} LIMIT {count}")
        rows = cur.fetchall()
        print("Uploading images to s3...")
        for row in tqdm(rows):
            row_uuid = row[6]
            row_apod_hdurl = row[5]
            if row_apod_hdurl != None:
                try:
                    # get image from HD url in bytes
                    row_image = main.get_image_for_s3_upload(row_apod_hdurl)
                    # upload image to s3
                    self.s3_client.upload_fileobj(row_image, self.s3_bucket, str(row_uuid))
                    # check if image was uploaded
                    self.s3_client.get_object(Bucket=self.s3_bucket, Key=str(row_uuid))
                    # update db to reflect that image has been uploaded to s3
                    cur.execute(f"UPDATE {table_name} SET in_s3 = {True} WHERE id = %s", (row_uuid,))
                    conn.commit()
                    s3_results += 1
                except Exception as e:
                    pass
            else:
                try:
                    # get image from normal url in bytes
                    row_image = main.get_image_for_s3_upload(row[4])
                    # upload image to s3
                    self.s3_client.upload_fileobj(row_image, self.s3_bucket, str(row_uuid))
                    # check if image was uploaded
                    self.s3_client.get_object(Bucket=self.s3_bucket, Key=str(row_uuid))
                    # update db to reflect that image has been uploaded to s3
                    cur.execute(f"UPDATE {table_name} SET in_s3 = {True} WHERE id = %s", (row_uuid,))
                    conn.commit()
                    s3_results += 1
                except Exception as e:
                    pass
        conn.close()
        return s3_results


class dbActions():
    def __init__(self, pg_db_name, pg_db_user, pg_db_password, pg_db_port, pg_table_name, apod_api_key, s3_bucket_name,
                 s3_access_key, s3_secret_key):
        self.pg_db_name = pg_db_name
        self.pg_db_user = pg_db_user
        self.pg_db_password = pg_db_password
        self.pg_db_port = pg_db_port
        self.pg_table_name = pg_table_name
        self.apod_api_key = apod_api_key
        self.s3_bucket_name = s3_bucket_name
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key


    def update_dbs(self, start_date, end_date):
        # only update DBs by 1-year increments
        difference_in_years = relativedelta(end_date, start_date).years
        for year in range(difference_in_years):
            print(f"Downloading and updating data for year {year}...")
            if year != difference_in_years:
                s_date = start_date + relativedelta(years=year)
                e_date = start_date + relativedelta(years=year+1)
            else:
                s_date = start_date + relativedelta(years=year)
                e_date = end_date
            # write a set of apod image metadata to postgres db
            pg_success = 0
            print(f"Making APOD request (start date={s_date}, end date={e_date})...")
            images = main.make_apod_request(self.apod_api_key, s_date, e_date)
            print(f"Writing images to PG DB (start date={s_date}, end date={e_date})...")
            for i in tqdm(images):
                if "copywright" not in i:
                    obj = pgObj(i)
                    pg_status = obj.write_to_db(self.pg_db_name, self.pg_db_user, self.pg_db_password, self.pg_db_port, self.pg_table_name)
                    pg_success += pg_status
            # write images to s3
            s3_success = 0
            s3 = s3Obj(self.s3_bucket_name, self.s3_access_key, self.s3_secret_key)
            s3_status = s3.upload_images(self.pg_db_name, self.pg_db_user, self.pg_db_password, self.pg_db_port, self.pg_table_name, len(images))
            s3_success += s3_status
            print(f"Database upload process complete!\nPG DB write attempts: {len(images)} images | successes: {pg_success}"
                  f" images\nS3 upload attempts: {len(images)} images | successes: {s3_success} images")


    def auto_data_update(self):
        # check the date coverage of the PG database
        db_obj = pgObj([])
        conn, cur = db_obj.connect_to_db(self.pg_db_name, self.pg_db_user, self.pg_db_password, self.pg_db_port)
        cur.execute(f"SELECT date FROM {self.pg_table_name} order by date ASC")
        date_list = cur.fetchall()
        # prioritize missing date chunks first (dates not included in between the first and last dates)
        # EX: if the min date is 1995-06-16 and max is 2015-07-01, then we will prioritize post 2015-07-01 date chunks,
        # then return to missing dates in between the date coverage
        # hard code apod start date (won't change)
        apod_start_date = '1995-06-16'
        apod_start_date_obj = datetime.strptime(apod_start_date, '%Y-%m-%d').date()
        # set the latest possible date (current date)
        apod_max_date = datetime.today().strftime('%Y-%m-%d')
        apod_max_date_obj = datetime.strptime(apod_max_date, '%Y-%m-%d').date()
        # get the earliest and latest date in the db (relying on DB query ordering)
        min_date = date_list[0][0]
        max_date = date_list[-1][0]
        # first check if there is a missing chunk before the min_date
        if min_date > apod_start_date_obj:
            print(f"Updating DBs with missing date chunk before {min_date}")
            self.update_dbs(apod_start_date_obj, min_date)
        if max_date < apod_max_date_obj:
            print(f"Updating DBs with missing date chunk after {max_date}")
            self.update_dbs(max_date, apod_max_date_obj)
        return date_list


if __name__ == "__main__":
    configs = toml.load("config.toml")
    # use convenience function to update dbs
    db = dbActions(configs['db_name'], configs['db_user'], configs['db_password'], configs['db_port'],
                   configs['db_table_name'], configs['api_key'], configs['s3_bucket'], configs['aws_access_key'],
                   configs['aws_secret_key'])
    db.auto_data_update()
