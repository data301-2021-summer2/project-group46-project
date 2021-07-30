import pandas as pd
import numpy as np

def changeStateToInt(state):
        if state == 'failed':
            return 0
        elif state == 'successful':
            return 1
        elif state == 'canceled':
            return 2
        elif state == 'live':
            return 3
        elif state == 'undefined':
            return 4
        elif state == 'suspended':
            return 5
        else:
            return -1

def getProjectsWithDurationInTimeRange(df, fromTime, toTime):
        fromTimeTD = pd.Timedelta(str(fromTime) + ' days')
        toTimeTD = pd.Timedelta(str(toTime) + ' days')
        df2 = df[(df['duration'] > fromTimeTD) & (df['duration'] < toTimeTD)]
        return df2

def getProjectsAsFailedOrSuccessful(df):
        # only keep projects that have a state of either 'failed', 'successful', or 'canceled'
        df2 = df[(df['stateInt'] == 0) | (df['stateInt'] == 1) | (df['stateInt'] == 2)]
        # change any 'canceled' projects to be considered as 'failed' projects
        # 0 -> means failed
        # 1 -> means successful
        # 2 -> means canceled
        df2 = df2.assign(stateInt=lambda x: np.where(x['stateInt'] != 2, x['stateInt'], 0))
        return df2
    
    

def checkSuccess(state):
    #This just checks to see if the project is successful.
    if state == "successful":
        return 1
    else:
        return -1

def load_and_process(url_or_path_to_csv_file, who):
        
        if who == "Jacob":
            #np.where(x.state == 'successful', 1, 0)

            # Main Method Chain For Data Analysis Pipeline
            dataFrame = (pd.read_csv(url_or_path_to_csv_file, nrows=1000)
                        .rename(columns={"name": "Name"})
                        .assign(stateInt=lambda x: x['state'].apply(changeStateToInt))
                        .assign(duration=lambda x: (pd.to_datetime(x['deadline']) - pd.to_datetime(x['launched'])))
                        .assign(percentFunded=lambda x: (x['usd_pledged_real']/x['usd_goal_real']*100))
                        .assign(durationInt=lambda x: x['duration'].dt.days)
                        .drop(['ID', 'usd pledged', 'goal'], axis=1)
                        .dropna())
            # Pipeline explanation:
            # read_csv: Creates a DataFram from the Kickstarter Project data csv file from the path given to the function
            # rename: Renames the columns so that they are easier to read
            # assign 1: creates a new column in the dataframe called 'stateInt' which is a mapping of the state columns values of
                      # successful/failure to integer values of 0 and 1 so that graphs can be made.
            # assign 2: Creates a new column called 'duration' which is the duration of each Kickstarter project.
                      # This is calculated by the difference between the 'launched' date and the 'deadline' date
            # assign 3: Creates a new column called 'percentFunded' to measure how close a project was to their funding goal. 
            # assign 4: Creates a new columns from the 'duration' column but has changed the type to int so that analysis can be
                        # done where numeric values are needed
                        # Referenced for help on converting date_time column to int column:
                        # https://stackoverflow.com/questions/25646200/python-convert-timedelta-to-int-in-a-dataframe
            # drop: Drops the columns that are not used in the data analysis
            # dropna: Drops the rows in the dataset that have a NaN value


            # For help of creating the 'duration' column: 
            # https://www.geeksforgeeks.org/convert-the-column-type-from-string-to-datetime-format-in-pandas-dataframe/
            # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.to_datetime.html


            return dataFrame

            # pandas functions reference:
            # drop: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.drop.html?highlight=drop#pandas.DataFrame.drop
            # dropna: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.dropna.html?highlight=dropna#pandas.DataFrame.dropna
            # apply: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.apply.html?highlight=apply#pandas.Series.apply
            # assign: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.assign.html?highlight=assign#pandas.DataFrame.assign
        
        elif who == "Rylan":
            #Removed each of the non-essential data types, then renamed the ones we had as well as removing any null data.
            df = (pd.read_csv(url_or_path_to_csv_file)
                  .drop(['category','ID','currency','deadline','goal','launched','pledged','backers','country',"usd pledged"],axis=1)
                  .rename(columns = {"usd_pledged_real": "pledged","usd_goal_real": "goal", "main_category":"category"})
                  .dropna()
                 )
            #Removed any rows with a pledge value lower than 1.
            df = df[df["pledged"]>=1]
            
            #Remove anything with a goal greater than 100000 to remove outliers, this was done the remove a few crazy outliers in the technology and journalism categories
            df = df[df["goal"]<=100000]
            
            #We then remove all rows that are an unusable state type like undefined or live
            df = df[(df['state'] == 'successful') | (df['state'] == 'canceled') | (df['state'] == 'suspended')]
            
            #Next we make a new category called plus_minus that measures the monitary success of a failed or succesful campaign
            df['plus_minus'] = (df.state.apply(checkSuccess) * df.pledged)
            
            return df