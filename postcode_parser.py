import os
from zipfile import ZipFile
from decouple import config
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import psycopg2

"""
A Script to Parse Ordnance Survey Postcode Polygon Data into a PSQL database
This script downloads raw data from a user-defined google drive and imports it to a user-defined database
"""

DATASETS_DIR = '{path to dataset}'

GDRIVE_SETTINGS_FILE = '{path to settings.yaml file for google drive}'
GDRIVE_TEAM_ID = 'gdrive id for the team id that data are downloaded from'
GDRIVE_FOLDER_ID = 'gdrive id for the folder containing postcode data'

DB_HOST = 'HOST'
DB_PORT = 'DATABASE_PORT'
DB_USER = 'DATABASE_USER'
DB_PASS = 'DATABASE_PASSWORD'
DB_NAME = 'DATABASE_NAME'
DB_CONN_STR = F'host={DB_HOST} port={DB_PORT} password={DB_PASS} user={DB_USER} dbname={DB_NAME}'

def create_service():
    """
    Creates a connection to google drive using the gdrive settings
    """
    gauth = GoogleAuth(settings_file=GDRIVE_SETTINGS_FILE)
    
    if gauth.credentials is None:
        # Authenticate if they're not there
        gauth.CommandLineAuth()
    elif gauth.access_token_expired:
        # Refresh them if expired
        gauth.Refresh()
    else:
        # Initialize the saved creds
        gauth.Authorize()
    # Save the current credentials to a file

    drive = GoogleDrive(gauth)
    return drive

def download_gdrive_folder(folder_id, dest_path, team_drive_id):
    """
    Downloads the contents of an entire gdrive folder with id = folder_id
    """

    drive = create_service()
    gdrive_list = drive.ListFile({'q': f"'{folder_id}' in parents and trashed=false",
        'teamDriveId': team_drive_id, 
        'corpora':"teamDrive",
        'includeTeamDriveItems': "true",
        'supportsAllDrives': "true"}).GetList()

    for i in range(len(gdrive_list)):
        file_i = gdrive_list[i]
        f = drive.CreateFile({'id':file_i['id']})
        f.GetContentFile(dest_path / file_i['title'])

def copy_from_csv(sql_command, csv_file):
    """
    Copies data from a csv file to an sql table
    Parameters:
        sql_command (str): the sql command that needs to be run to copy the csv file
                            e.g. "COPY table_name FROM stdin DELIMITER ',' CSV HEADER;"
        csv_file (file): the file object that will read into the db
    """
    try:
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, keepalives_idle=10)
        cursor = conn.cursor()

        # execute the SQL command to copy from the csv file
        cursor.copy_expert(sql_command,csv_file)

        # close communication with the PostgreSQL database server
        cursor.close()

        # commit the changes
        conn.commit()
        conn.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


class PostcodeParser():


    def __init__(self):
        # Initialise all variables
        self.base_path = DATASETS_DIR
        self.staging_table = 'postcode_staging'
        self.geom_table = 'postcode_polygons'
        self.target_table = 'postcode'
        self.vstreet_table = 'vstreet_table'
        self.vstreet_target = 'vstreetlookup'
        self.folder_id = GDRIVE_FOLDER_ID
        self.team_drive_id= GDRIVE_TEAM_ID

    def prepare(self):
        # look for the directories necessary for the data to be parsed
        os.makedirs(self.base_path,exist_ok=True)


        # download postcode data from google drive
        download_gdrive_folder(self.folder_id, self.base_path)
        for z in os.listdir(self.base_path):
            if z.endswith('.zip'):
                self.zip_file = z
            if z.endswith('.csv'):
                self.header_file = z

        return False,''

    def parse_polys(self):
        """
        Creates a new table with postcode names and polygons from the supplied shapefiles
        """

        with ZipFile(self.base_path / self.zip_file,'r') as z:
            # Search through the zip file for nested zip files
            ZIP_FILES = [f for f in z.namelist() if f.endswith('.zip')]

            # Iterate through the nested zip files (these contain the shapefiles)
            for z_file in ZIP_FILES:

                # define the shapefile directory
                SHP_DIR = self.base_path / self.zip_file / z_file

                # open the zipfile
                with z.open(z_file) as zp:

                    # open the nested zipfile
                    with ZipFile(zp,'r') as z_shp:
                        # Search for shapefiles to import
                        SHP_FILES = [f for f in z_shp.namelist() if f.endswith('.shp')]
                        
                        # Drop existing table
                        SQL_COMMANDS = (
                        f'''
                        DROP TABLE IF EXISTS {self.staging_table};
                        ''',
                        )
            
                        exec_sql_statements(SQL_COMMANDS)
                        
                        i = 0

                        for file in SHP_FILES:
                            # Iterate through shapefiles
                            FILE_PATH = SHP_DIR / file

                            # Use vsizip driver to import directly from zip files
                            filepath = '/vsizip/vsizip/' + str(FILE_PATH)

                            # Import using ogr2ogr
                            print('Parsing Postcode >>',file)
                            command = f'ogr2ogr -f "PostgreSQL" PG:"{DB_CONN_STR}" "{filepath}" -nln {self.staging_table} -nlt MULTIPOLYGON  -append -progress -t_srs "EPSG:4326"'
                            os.system(command)
                            i += 1

        # Clean up/drop tables to reformat to required format
        SQL_COMMANDS = (
                f'''
                ALTER TABLE {self.staging_table}
                RENAME COLUMN ogc_fid TO id;
                ''',

                f'''
                ALTER TABLE {self.staging_table}
                RENAME COLUMN wkb_geometry TO polygon;
                ''',

                f'''
                CREATE TABLE IF NOT EXISTS {self.geom_table} AS
                SELECT id,postcode,pc_area,polygon FROM {self.staging_table};
                ''',

                f'''
                ALTER TABLE {self.geom_table}
                ADD PRIMARY KEY (id);
                ''',
                f'''
                ALTER TABLE {self.geom_table}
                ALTER COLUMN polygon TYPE geography;
                ''',
                f'''
                DROP TABLE {self.staging_table};
                '''
            )

        print("renaming columns...")
        exec_sql_statements(SQL_COMMANDS)
    
    def parse_info(self):
        """
        Creates a new table with supplementary postcode information from the supplied csvs
        """

        # find the column headers from the supplied header file
        with open(self.base_path / self.header_file) as header_file:
            column_names = header_file.readlines()[-1].split(',')

        with ZipFile(self.base_path / self.zip_file, 'r') as z:
            # Search through zip file for csvs
            CSV_FILES = [f for f in z.namelist() if f.endswith('.csv') and f.startswith('Code-Point/Data/CSV')]

            # Drop the staging table
            SQL_COMMANDS = (
                            f'''
                            DROP TABLE IF EXISTS {self.staging_table};
                            ''',
                            )
                
            exec_sql_statements(SQL_COMMANDS)
            i = 0

            # Iterate through csv files
            for csv_file in CSV_FILES:
                FILE_PATH = self.base_path / self.zip_file / csv_file

                # use vsizip driver to open zip file
                filepath = '/vsizip/' + str(FILE_PATH)

                # Import using ogr2ogr
                print('Parsing Postcode >>',csv_file)
                command = f'ogr2ogr -f "PostgreSQL" PG:"{DB_CONN_STR}" "{filepath}" -nln {self.staging_table} -append -progress'
                os.system(command)

                i += 1

            SQL_COMMANDS=()

            # Rename columns using column headers
            fields = f'{self.geom_table}.id, {self.geom_table}.postcode, {self.geom_table}.pc_area, '
            for j in range(len(column_names)):
                SQL_COMMANDS = SQL_COMMANDS + (f'''
                ALTER TABLE {self.staging_table}
                RENAME COLUMN field_{j+1} TO {column_names[j].lower()};
                ''',)
                if column_names[j].lower() not in ['postcode','eastings','northings', 'delivery_points_used_to_create_the_cplc']:
                    fields = fields + f'{self.staging_table}.' + column_names[j].lower() + ', '

            fields = fields + f'{self.geom_table}.polygon'

            # Create and reformat final table, drop staging/temporary tables
            SQL_COMMANDS = SQL_COMMANDS + (f'''
            DROP TABLE IF EXISTS {self.target_table};
            ''',
            f'''
            CREATE TABLE {self.target_table} AS
            SELECT {fields} FROM {self.staging_table}
            RIGHT JOIN {self.geom_table}
            ON {self.geom_table}.postcode={self.staging_table}.postcode;
            ''',
            f'''
            ALTER TABLE {self.target_table}
            ALTER COLUMN positional_quality_indicator TYPE int USING positional_quality_indicator::integer;
            ''',
            f'''
            ALTER TABLE {self.target_table}
            ALTER COLUMN po_box_indicator TYPE varchar(2);
            ''',
            f'''
            ALTER TABLE {self.target_table}
            ALTER COLUMN total_number_of_delivery_points TYPE int USING total_number_of_delivery_points::integer;
            ''',
            f'''
            ALTER TABLE {self.target_table}
            ALTER COLUMN domestic_delivery_points TYPE int USING domestic_delivery_points::integer;
            ''',
            f'''
            ALTER TABLE {self.target_table}
            ALTER COLUMN non_domestic_delivery_points TYPE int USING non_domestic_delivery_points::integer;
            ''',
            f'''
            ALTER TABLE {self.target_table}
            ALTER COLUMN po_box_delivery_points TYPE int USING po_box_delivery_points::integer;
            ''',
            f'''
            ALTER TABLE {self.target_table}
            ALTER COLUMN matched_address_premises TYPE int USING matched_address_premises::integer;
            ''',
            f'''
            ALTER TABLE {self.target_table}
            ALTER COLUMN unmatched_delivery_points TYPE int USING unmatched_delivery_points::integer;
            ''',
            f'''
            ALTER TABLE {self.target_table}
            ALTER COLUMN country_code TYPE varchar(16);
            ''',
            f'''
            ALTER TABLE {self.target_table}
            ALTER COLUMN nhs_regional_ha_code TYPE varchar(16);
            ''',
            f'''
            ALTER TABLE {self.target_table}
            ALTER COLUMN nhs_ha_code TYPE varchar(16);
            ''',
            f'''
            ALTER TABLE {self.target_table}
            ALTER COLUMN admin_county_code TYPE varchar(16);
            ''',
            f'''
            ALTER TABLE {self.target_table}
            ALTER COLUMN admin_district_code TYPE varchar(16);
            ''',
            f'''
            ALTER TABLE {self.target_table}
            ALTER COLUMN admin_ward_code TYPE varchar(16);
            ''',
            f'''
            ALTER TABLE {self.target_table}
            ALTER COLUMN postcode_type TYPE varchar(2);
            ''',
            f'''
            ALTER TABLE {self.target_table}
            ADD PRIMARY KEY (id);
            ''',

            f'''
            DROP TABLE {self.geom_table};
            ''',

            f'''
            DROP TABLE {self.staging_table};
            '''
            )
            
            print("merging tables...")
            exec_sql_statements(SQL_COMMANDS)

    def parse_vstreets(self):
        """
        Creates a new table with supplementary vertical street information from the supplied txts
        """

        with ZipFile(self.base_path / self.zip_file, 'r') as z:
            # Search through zip file for txt files
            TXT_FILES = [f for f in z.namelist() if f.endswith('.TXT') and f.startswith('Polygons/Data/VERTICAL_STREETS')]

            # Drop the target table
            SQL_COMMANDS = (
                            f'''
                            DROP TABLE IF EXISTS {self.vstreet_table};
                            DROP TABLE IF EXISTS {self.vstreet_target};
                            ''',
                            )
                
            exec_sql_statements(SQL_COMMANDS)

            i = 0

            # Separate SQL commands for cleaning, creating and dropping tables
            SQL_COMMANDS = (f'''
                CREATE TABLE {self.vstreet_table} (
                    postcode CHARACTER VARYING(8),
                    vstreet_ref CHARACTER VARYING(8)
                );
                ''',
                )

            exec_sql_statements(SQL_COMMANDS)

            # Iterate through txt files
            for txt_file in TXT_FILES:
                with z.open(txt_file) as txt:
                    # use unzip to open zip files and extract txt files
                    copy_from_csv(f'''COPY {self.vstreet_table} FROM STDIN DELIMITER ',' CSV QUOTE AS '"';''',txt)

            SQL_COMMANDS_CLEAN = (
                f'''
                ALTER TABLE {self.vstreet_table}
                ADD COLUMN id BIGSERIAL;
                ''',
                f'''
                CREATE TABLE {self.vstreet_target} AS
                SELECT id, postcode, vstreet_ref FROM {self.vstreet_table};
                ''',
                f'''
                ALTER TABLE {self.vstreet_target}
                ADD PRIMARY KEY (id);
                '''
            )

            SQL_COMMANDS_DROP = (
                f'''
                DROP TABLE {self.vstreet_table};
                ''',)

            exec_sql_statements(SQL_COMMANDS_CLEAN)
            exec_sql_statements(SQL_COMMANDS_DROP)

    def index_tables(self):
        """
        Create psql indexes on all the tables in the db
        """
        SQL_COMMANDS = (
                    f'''
                    CREATE INDEX core_postcode_polygon_id
                        ON public.core_postcode USING gist
                        (polygon)
                        TABLESPACE pg_default;
                    ''',
                    f'''
                    CREATE INDEX core_postcode_postcode_36243775
                        ON public.core_postcode USING btree
                        (postcode COLLATE pg_catalog."default" ASC NULLS LAST)
                        TABLESPACE pg_default;
                    ''',
                    f'''
                    CREATE INDEX core_postcode_postcode_36243775_like
                        ON public.core_postcode USING btree
                        (postcode COLLATE pg_catalog."default" varchar_pattern_ops ASC NULLS LAST)
                        TABLESPACE pg_default;
                    ''',
                    f'''
                    CREATE INDEX core_vstreetlookup_postcode_4a092989
                        ON public.core_vstreetlookup USING btree
                        (postcode COLLATE pg_catalog."default" ASC NULLS LAST)
                        TABLESPACE pg_default;
                    ''',
                    f'''
                    CREATE INDEX core_vstreetlookup_postcode_4a092989_like
                        ON public.core_vstreetlookup USING btree
                        (postcode COLLATE pg_catalog."default" varchar_pattern_ops ASC NULLS LAST)
                        TABLESPACE pg_default;
                    ''',
                    f'''
                    CREATE INDEX core_vstreetlookup_vstreet_ref_b613262c
                        ON public.core_vstreetlookup USING btree
                        (vstreet_ref COLLATE pg_catalog."default" ASC NULLS LAST)
                        TABLESPACE pg_default;
                    ''',
                    f'''
                    CREATE INDEX core_vstreetlookup_vstreet_ref_b613262c_like
                        ON public.core_vstreetlookup USING btree
                        (vstreet_ref COLLATE pg_catalog."default" varchar_pattern_ops ASC NULLS LAST)
                        TABLESPACE pg_default;
                    ''',
                    )
                
        exec_sql_statements(SQL_COMMANDS)

    def cleanup(self):
        for f in os.listdir(self.base_path):
            os.remove(self.base_path / f)

    def parse(self):
        err, msg = self.prepare()
        if err: return err, msg

        self.parse_polys()
        self.parse_info()
        self.parse_vstreets()
        self.index_tables()
        self.cleanup()

        return False, ''
        
if __name__ == "__main__":
    parser = PostcodeParser()
    parser.parse()
