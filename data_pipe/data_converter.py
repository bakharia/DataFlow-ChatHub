###################################
###################################
# Author: Bakharia
# Date: 01/01/2024
# Project: UniPie
# Version: 0.1
# Functionality: Preliminary script 
#                to convert data to 
#                db
###################################
###################################

###################################
###################################
# LIBRARIES
import os
from dotenv import load_dotenv

from glob import glob
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
# from pymongo import MongoClient
###################################
###################################

# uri = postgresql://bakharia:2BGpfu5iyMze@ep-mute-king-05475697.us-east-1.aws.neon.tech/UniPie?sslmode=require

class data_pipe:
    '''
    data class
    '''

    filepaths = []
    tables = {
        'University': pd.DataFrame(),
        'Programme': pd.DataFrame(),
        'ProgrammeDescription': pd.DataFrame(),
        'CourseDescription': pd.DataFrame(),
        'TestType': pd.DataFrame()
    }
    tables_headers = {
        'University': [
            'uni_name', 'location', 'founded',
            'website', 'overall_ranking', 'International_Students',
            'Female_Male_Ratio', 'total_students', 'athletics',
            'contact', 'research_funding', 'airport_transportation',
            'bus_availability', 'train_station_distance', 'nearby_shopping_areas',
            'campus_facilities', 'emergency_services', 'student_housing',
            'Living costs', 'student_clubs_organizations', 'Public_Private'
            ],
        'Programme': [
            'programme_name', 'duration', 'description', 'fees (annual)', 
            'admission_requirements', 'degree_awarded', 'mode_of_study', 
            'on_off_campus', 'scholarships', 'language_of_instruction',
            'internship_opportunities', 'study_abroad_opportunities'
            ],
        'ProgrammeDescription': [
            'programme_name', 'overview', 'website', 
            'learning_objectives', 'program_structure', 
            'specialisations', 'career_opportunities'
        ],
        'CourseDescription': [
            'programme_name', 'course_name', 'course_description', 
            'course_objectives', 'core_elective'
        ],
        'TestType': [
            'programme_name', 'test_name', 'average_score', 'minimum_score'
        ]
    }
    uni_name = ''

    #Helper Function
    def load_files(self):
        '''
        Load all files from the folder `uni_data` ending with the extensions `.xlsx`
        '''    
        for f in glob('uni_data/*.xlsx'):
            self.filepaths.append(f)
    
    def read_table(self, file_path: str, table: str) -> pd.DataFrame:
        '''
        Reading in the table
        '''

        print(table)

        if table == 'University':
            table_df = pd.DataFrame(columns=self.tables_headers[table])
            temp = pd.read_excel(
                io=file_path,
                header=None,
                sheet_name=table,
                skiprows=0
            ).iloc[:, :2]

            temp = temp[~(temp.iloc[:, 0].isna())]

            temp = temp.T
            temp.columns = temp.iloc[0, :].apply(lambda x: str(x).strip())  # Strip leading/trailing whitespaces
            temp = temp.iloc[1:, :]

            # Convert Index object to a list before using .str
            temp_columns_list = temp.columns.str.lower()

            # Match columns case-insensitively and ignore underscores
            matched_columns = [next((col for col in self.tables_headers[table] if str(col).replace('_', '').lower() == str(temp_col).replace('_', '').lower()), None) for temp_col in temp_columns_list]
            
            self.uni_name = temp.iloc[0, 0]

            # print(self.uni_name)
            # print(temp)

            # Copy values from temp to table_df based on matched columns
            for col in matched_columns:
                if col is not None:
                    table_df[col] = temp[col].values

            print(table_df)

            return table_df
        
        elif table == 'Programme':
            table_df = pd.read_excel(
                io= file_path,
                sheet_name= table,
                names= self.tables_headers[table],
                header= 2 if 'Munich' == file_path.split(' ')[-1].split('.')[0] else None,
                index_col=None,
                engine='openpyxl'
            )
            # table_df.reset_index(drop= False, inplace= True)
            # print(table_df.columns, self.tables_headers[table], sep='\n')
            # table_df.columns = self.tables_headers[table]
            table_df = table_df.loc[
                ~(table_df.programme_name.isin(self.tables_headers[table]) | table_df.degree_awarded.isna())
            ]
            # table_df
            table_df['uni_name'] = self.uni_name
            return table_df
        
        elif table in ['ProgrammeDescription', 'CourseDescription', 'TestType']:
            table_df = pd.read_excel(
                io= file_path,
                sheet_name= table,
                names= self.tables_headers[table],
                header= None,
                index_col=None,
                engine='openpyxl'
            )
            # table_df.reset_index(drop= False, inplace= True)
            # print(table_df.columns, self.tables_headers[table], sep='\n')
            # table_df.columns = self.tables_headers[table]
            table_df.iloc[:, 0] = table_df.iloc[:, 0].ffill()
            table_df = table_df.loc[
                ~(table_df.programme_name.isin(self.tables_headers[table]) | (table_df.programme_name.isna()))
            ]
            # table_df
            table_df['uni_name'] = self.uni_name
            return table_df.drop_duplicates()

        return pd.read_excel(
            io= file_path,
            sheet_name= table,
            names= self.tables_headers[table],
            header= None
        )
        # return pd.DataFrame()
    
    def connect_to_postgres(self):
        '''
        Function to connect to PostgreSQL
        '''
        load_dotenv()
        
        URI = os.getenv('POSTGRES_URI')
        print(URI)

        # Create an SQLAlchemy engine from the uri
        engine = create_engine(URI)

        # Connect to the PostgreSQL database using psycopg2
        conn = psycopg2.connect(URI)

        return conn, engine
    
    def create_tables_in_postgres(self, conn):
        '''
        function to create tables in PostgreSQL
        '''
        cursor = conn.cursor()

        # Create University table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS University (
                uni_name VARCHAR PRIMARY KEY,
                location VARCHAR,
                founded INTEGER,
                website VARCHAR,
                overall_ranking INTEGER,
                International_Students INTEGER,
                Female_Male_Ratio FLOAT,
                total_students INTEGER,
                athletics VARCHAR,
                contact VARCHAR,
                research_funding FLOAT,
                airport_transportation VARCHAR,
                bus_availability VARCHAR,
                train_station_distance FLOAT,
                nearby_shopping_areas VARCHAR,
                campus_facilities VARCHAR,
                emergency_services VARCHAR,
                student_housing VARCHAR,
                "Living costs" FLOAT,
                student_clubs_organizations VARCHAR,
                Public_Private VARCHAR
            );
        ''')

        # Add unique constraint to uni_name in University table
        # cursor.execute('''
        #     ALTER TABLE University
        #     ADD CONSTRAINT uni_name_unique UNIQUE (uni_name);
        # ''')

        # Create Programme table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Programme (
                uni_name VARCHAR,
                programme_name VARCHAR PRIMARY KEY,
                duration VARCHAR,
                description VARCHAR,
                "fees (annual)" VARCHAR,
                admission_requirements VARCHAR,
                degree_awarded VARCHAR,
                mode_of_study VARCHAR,
                on_off_campus VARCHAR,
                scholarships VARCHAR,
                language_of_instruction VARCHAR,
                internship_opportunities VARCHAR,
                study_abroad_opportunities VARCHAR,
                FOREIGN KEY (uni_name) REFERENCES University(uni_name)
            );
        ''')

        # Create ProgrammeDescription table (similar modifications for other tables)

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ProgrammeDescription (
                uni_name VARCHAR,
                programme_name VARCHAR PRIMARY KEY,
                overview VARCHAR,
                website VARCHAR,
                learning_objectives VARCHAR,
                program_structure VARCHAR,
                specialisations VARCHAR,
                career_opportunities VARCHAR,
                FOREIGN KEY (uni_name) REFERENCES University(uni_name),
                FOREIGN KEY (programme_name) REFERENCES Programme(programme_name)
            );
        ''')

        # Create CourseDescription table (similar modifications for other tables)

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS CourseDescription (
                uni_name VARCHAR,
                programme_name VARCHAR,
                course_name VARCHAR PRIMARY KEY,
                course_description VARCHAR,
                course_objectives VARCHAR,
                core_elective VARCHAR,
                FOREIGN KEY (uni_name) REFERENCES University(uni_name),
                FOREIGN KEY (programme_name) REFERENCES Programme(programme_name)
            );
        ''')

        # Create TestType table (similar modifications for other tables)

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS TestType (
                uni_name VARCHAR,
                programme_name VARCHAR,
                test_name VARCHAR,
                average_score FLOAT,
                minimum_score FLOAT,
                PRIMARY KEY (uni_name, programme_name, test_name),
                FOREIGN KEY (uni_name) REFERENCES University(uni_name),
                FOREIGN KEY (programme_name) REFERENCES Programme(programme_name)
            );
        ''')

        conn.commit()


    def connect_to_postgres_and_insert(self, table, table_name):
        '''
        function to connect to PostgreSQL and insert data
        '''
        conn, engine = self.connect_to_postgres()

        # Insert data into PostgreSQL
        self.create_tables_in_postgres(conn)

        # Insert data into the created table
        table.to_sql(table_name, con=engine, if_exists='replace', index=False)

        conn.commit()

        # Close the connection
        conn.close()

    def load_data(self):
        '''
        Driver function
        '''
        for f in self.filepaths[:]:
            print(f)
            for table in self.tables.keys():
                
                self.tables[table] = pd.concat([self.tables[table], self.read_table(file_path=f, table=table)])
                
                # table_name = f"{table}"
                # self.connect_to_postgres_and_insert(self.tables[table], table_name)

    # def load_data(self) -> None:
    #     '''
    #     Driver function
    #     '''
    #     for f in self.filepaths[:]:
    #         print(f)
    #         for table in self.tables.keys():
                
    #             # self.tables[table].append(self.read_table(file_path = f, table = table))
    #             self.tables[table] = pd.concat([self.tables[table], self.read_table(file_path=f, table= table)])
    
    # def connect_to_mongo(self, table:pd.DataFrame, table_name: str) -> None:
        # '''
        # function to connect to mongo and push data
        # '''
        # password = 'e1ygv3sZmCg4pE81'
        # uri = f"mongodb+srv://bakharia1:{password}@unipie.wzlt9kr.mongodb.net/?retryWrites=true&w=majority"
        # client = MongoClient(uri)
        # db = client['UniPie']
        # collection = db[table_name]

        # data_to_insert = table.to_dict(orient='records')
        # collection.insert_many(data_to_insert)

        # client.close()

        # client = MongoClient(uri, server_api=ServerApi('1'))
        # # Send a ping to confirm a successful connection
        # try:
        #     client.admin.command('ping')
        #     print("Pinged your deployment. You successfully connected to MongoDB!")
        # except Exception as e:
        #     print(e)
        
    def __init__(self) -> None:
        self.load_files()
        self.load_data()
        # print(self.tables)
        for name, table in self.tables.items():

            table.reset_index(drop= True, inplace= True)
            table.drop_duplicates(inplace= True)
            
            # self.connect_to_mongo(table, name)
            print(table)
            self.connect_to_postgres_and_insert(table, name)


data_pipe()