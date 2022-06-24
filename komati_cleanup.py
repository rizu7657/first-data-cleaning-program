# Import libraries
#import mysql.connector
try:    
    from logging import exception
    from sqlalchemy import create_engine
    import pandas as pd
    import pathlib
    import datetime
    
except:
    import os
    os.system('python -m pip install sqlalchemy mysql.connector pandas')
    from logging import exception
    from sqlalchemy import create_engine
    import pandas as pd
    import pathlib
    import datetime

# Create database connection
engine = create_engine("mysql+mysqlconnector://{uname}:{pword}@localhost:3306/{db_name}")

def main():
    # The main function of the program 
                       
    def check_condition(vehicle):
        # This function is the main function used to check the condition of a vehicle and take steps to clean it

        def check_query(veh_reg):
            # This function gets the required information from MySQL to check conditions

            # The SQL query to be executed to obtain the necessary information
            query = ("""
            SELECT a.*, b.id as acc_veh_id, b.acc_id, b.veh_id, d.acc_name, c.total
            FROM vehicles a 
            LEFT JOIN acc_veh b ON a.id = b.veh_id 
            LEFT JOIN ( SELECT acc_veh_id, COUNT(acc_veh_id) AS total FROM transactions GROUP BY acc_veh_id) c ON c.acc_veh_id = b.id 
            LEFT JOIN accounts d ON b.acc_id = d.id
            WHERE a.veh_reg = '{}' """.format(veh_reg))

            # Execute the query and place it into a dataframe to better filter it
            result_df = pd.read_sql_query(query, engine)

            # Return the result
            return(result_df)
        def move_transactions(inactive_ids, active_id):
            # Define function to move transactions from inactive vehicle to active account vehicle
            # or move from account vehicles to other account vehicle

            # The SQL query to be executed to move transactions from a list of account vehicles
            # to a specified account vehicle.
            query = """
                    UPDATE transactions
                    SET acc_veh_id = {} #Acctive account vehicle
                    WHERE acc_veh_id in {} #Inactive cash vehicles
                    """.format(int(active_id), tuple(map(int, inactive_ids))).replace(",)",")")
            
            try:
                t_moved = int(db_vehicles[db_vehicles['acc_veh_id'].isin(inactive_ids)]['total'].sum())
                print('Moving {} transactions!'.format(t_moved))
                logs.write('-> Moving {} transactions from inactive {} to correct account {} !\n'.format(t_moved, inactive_ids, active_id))
                engine.execute(query)
                return(t_moved)
                
            except Exception as logs_exception:
                path = pathlib.Path('logs_errors.txt')
                print('Adding exception to error logs - ' + str(path.absolute()) + str(logs_exception))

                try:
                    errors = open('logs_errors.txt', 'a')
                    ts = str(datetime.datetime.now())
                    errors.write(ts + ': ' + str(logs_exception) + '\n')
                    errors.close()
                except Exception as error_logs_exception:
                    print(error_logs_exception)
                    exit()       
        def move_link(veh_acc_id, acco_id):
            # This function is used to move account vehicles from one account to another.

            query = """
                    update acc_veh
                    set acc_id = {}
                    where id = {}
                    """.format(acco_id, veh_acc_id)
                    
            try:
                
                logs.write('-> Moving vehicle {} to correct account {} !\n'.format(veh_acc_id, acco_id))
                engine.execute(query)
                print('Vehicle moved to correct account!')
                
            except Exception as logs_exception:
                path = pathlib.Path('logs_errors.txt')
                print('Adding exception to error logs - ' + str(path.absolute()) + str(logs_exception))

                try:
                    errors = open('logs_errors.txt', 'a')
                    ts = str(datetime.datetime.now())
                    errors.write(ts + ': ' + str(logs_exception) + '\n')
                    errors.close()
                    
                except Exception as error_logs_exception:
                    print(error_logs_exception)
                    exit()
        def check_active(vehicle_id):
            # Function to check whether the vehicle is active or not.

            query = ("""
                SELECT *
                FROM vehicles
                WHERE id = '{}' """.format(vehicle_id))
            result_df = pd.read_sql_query(query, engine)

            if result_df[result_df['id'] == vehicle_id]['veh_active'].iloc[0] == 0:
                query = ("""
                update vehicles
                set veh_active = 1
                where id = '{}'
                """.format(vehicle_id))

                try:
                    logs.write('-> Making vehicle {} - {} active!\n'.format(vehicle, vehicle_id))
                    engine.execute(query)

                except Exception as logs_exception:
                    path = pathlib.Path('logs_errors.txt')
                    print('Adding exception to error logs - ' + str(path.absolute()) + str(logs_exception))

                    try:
                        errors = open('logs_errors.txt', 'a')
                        ts = str(datetime.datetime.now())
                        errors.write(ts + ': ' + str(logs_exception) + '\n')
                        errors.close()
                    except Exception as error_logs_exception:
                        print(error_logs_exception)
                        exit()
            else:
                return
        def delete_records(veh_ids, acc_veh_ids):
            # This function uses a list of account vehicles and a list of vehicles to delete the duplicate entries.
            if (len(veh_ids) != 0) & (len(acc_veh_ids) != 0):
                    
                query = """
                DELETE FROM acc_veh WHERE id IN {}
                """.format(tuple(map(int, acc_veh_ids))).replace(",)",")")
                
                try:
                    print('Deleting duplicate account vehicles!')
                    ts = str(datetime.datetime.now())
                    logs.write('-> {}: Deleting duplicate account vehicles {}\n'.format(ts, acc_veh_ids))
                    engine.execute(query)
                    
                except Exception as logs_exception:
                    path = pathlib.Path('logs_errors.txt')
                    print('Adding exception to error logs - ' + str(path.absolute()) + str(logs_exception))

                    try:
                        errors = open('logs_errors.txt', 'a')
                        ts = str(datetime.datetime.now())
                        errors.write(ts + ': ' + str(logs_exception) + ' while deleting account vehicles.\n')
                        errors.close()
                        exit()
                        
                    except Exception as error_logs_exception:
                        print(error_logs_exception)
                        exit()
                        
                query = """
                DELETE FROM vehicles WHERE id IN {}
                """.format(tuple(veh_ids)).replace(",)",")")
                
                try:
                    print('Deleting duplicate vehicles!')
                    ts = str(datetime.datetime.now())
                    logs.write('-> {}: {}: Deleting duplicate vehicles {}\n'.format(ts, vehicle, veh_ids))
                    engine.execute(query)
                    
                except Exception as logs_exception:
                    path = pathlib.Path('logs_errors.txt')
                    print('Adding exception to error logs - ' + str(path.absolute()) + str(logs_exception))
                    
                    try:
                        errors = open('logs_errors.txt', 'a')
                        ts = str(datetime.datetime.now())
                        errors.write(ts + ': ' + str(logs_exception) + ' while deleting vehicles.\n')
                        errors.close()
                        exit()
                        
                    except Exception as error_logs_exception:
                        print(error_logs_exception)
                        exit()      

            elif (len(acc_veh_ids) == 0):
                query = """
                DELETE FROM vehicles WHERE id IN {}
                """.format(tuple(veh_ids)).replace(",)",")")
                
                try:
                    print('Deleting duplicate vehicles!')
                    ts = str(datetime.datetime.now())
                    logs.write('-> {}: {}: Deleting duplicate vehicles {}\n'.format(ts, vehicle, veh_ids))
                    engine.execute(query)
                    
                except Exception as logs_exception:
                    path = pathlib.Path('logs_errors.txt')
                    print('Adding exception to error logs - ' + str(path.absolute()) + str(logs_exception))
                    
                    try:
                        errors = open('logs_errors.txt', 'a')
                        ts = str(datetime.datetime.now())
                        errors.write(ts + ': ' + str(logs_exception) + ' while deleting vehicles.\n')
                        errors.close()
                        exit()
                        
                    except Exception as error_logs_exception:
                        print(error_logs_exception)
                        exit() 
            elif (len(veh_ids) == 0):
                query = """
                DELETE FROM acc_veh WHERE id IN {}
                """.format(tuple(map(int, acc_veh_ids))).replace(",)",")")
                
                try:
                    print('Deleting duplicate account vehicles!')
                    ts = str(datetime.datetime.now())
                    logs.write('-> {}: Deleting duplicate account vehicles {}\n'.format(ts, acc_veh_ids))
                    engine.execute(query)
                    
                except Exception as logs_exception:
                    path = pathlib.Path('logs_errors.txt')
                    print('Adding exception to error logs - ' + str(path.absolute()) + str(logs_exception))

                    try:
                        errors = open('logs_errors.txt', 'a')
                        ts = str(datetime.datetime.now())
                        errors.write(ts + ': ' + str(logs_exception) + ' while deleting account vehicles.\n')
                        errors.close()
                        exit()
                        
                    except Exception as error_logs_exception:
                        print(error_logs_exception)
                        exit()
            else:
                print('Lists for delete function were empty')
                exit()
        def add_vehicle(vehicle, accountid):
            # This function is used to add a vehicle that is not on the system.
            # First insert the vehicle into the vehicles table
            query = """
            INSERT INTO vehicles (veh_reg, veh_active)
            VALUES ('{}', 1)
            """.format(vehicle)

            try:
                logs.write('-> adding vehicle {} to vehicles table.\n'.format(vehicle))
                engine.execute(query)
                print('Vehicle added to system.')
            except Exception as logs_exception:
                path = pathlib.Path('logs_errors.txt')
                print('Adding exception to error logs - ' + str(path.absolute()) + str(logs_exception))

                try:
                    errors = open('logs_errors.txt', 'a')
                    ts = str(datetime.datetime.now())
                    errors.write(ts + ': ' + str(logs_exception) + '\n')
                    errors.close()
                    
                except Exception as error_logs_exception:
                    print(error_logs_exception)
                    exit()

            # Get vehicle ID
            query = """
            SELECT id from vehicles where veh_reg = '{}' """.format(vehicle)

            try:
                result_df = pd.read_sql_query(query, engine)  
            except Exception as logs_exception:
                path = pathlib.Path('logs_errors.txt')
                print('Adding exception to error logs - ' + str(path.absolute()) + str(logs_exception))
                try:
                    errors = open('logs_errors.txt', 'a')
                    ts = str(datetime.datetime.now())
                    errors.write(ts + ': ' + str(logs_exception) + '\n')
                    errors.close()
                    
                except Exception as error_logs_exception:
                    print(error_logs_exception)
                    exit()

            if len(result_df) > 1:
                print('Error! More than one vehicle found after inserting it.')
                try:    
                    errors = open('logs_errors.txt', 'a')
                    ts = str(datetime.datetime.now())
                    errors.write(ts + ': More than one vehicle found after inserting it.\n')
                    errors.close()  
                except Exception as error_logs_exception:
                    print(error_logs_exception)
                    exit()
            elif len(result_df) == 1:
                # get the veh_id from result dataframe
                veh_id = result_df['id'].iloc[0]

                # Add vehicle to the correct account
                query = """
                INSERT INTO acc_veh (acc_id, veh_id)
                VALUES ({}, {})
                """.format(accountid, veh_id)

                # insert the vehicle
                try:
                    logs.write('-> adding vehicle {} to account vehicles table.\n'.format(vehicle))
                    engine.execute(query)
                    print('Vehicle added to the system.')
                except Exception as logs_exception:
                    path = pathlib.Path('logs_errors.txt')
                    print('Adding exception to error logs - ' + str(path.absolute()) + str(logs_exception))

                    try:
                        errors = open('logs_errors.txt', 'a')
                        ts = str(datetime.datetime.now())
                        errors.write(ts + ': ' + str(logs_exception) + '\n')
                        errors.close()
                    except Exception as error_logs_exception:
                        print(error_logs_exception)
                        exit()
            else:
                print('The vehicle did not insert properly into vehicles table.')
                logs.write('-> vehicle did not insert properly into vehicles table. Try rerunning the program')
        def add_processed(condition, active_id = 0, t_moved = 0):
            processed = pd.read_pickle('processed.pickle')
            x = pd.DataFrame({'vehicle_registration': [vehicle],
                                        'acc_no': [account_id], 
                                        'active_id': [active_id], 
                                        'duplicate_veh_count': [(len(db_vehicles) - 1)], 
                                        'duplicate_acc_veh_count': [(len(db_vehicles[db_vehicles['acc_veh_id'].notna()])-1)], 
                                        'transactions_on_cash': [len(db_vehicles[(db_vehicles['acc_id'] == 1) & db_vehicles['total'].notna()])], 
                                        'transactions_on_account': [len(db_vehicles[(db_vehicles['acc_id'] == account_id) & db_vehicles['total'].notna()])],
                                        'transactions_moved': [t_moved],
                                        'condition': [condition]})
            processed = pd.concat([processed, x], ignore_index=True)

           

            # Export processed
            print('-> Exporting processed list to spreadsheet')
            try:
                processed.to_pickle('processed.pickle')
                processed.to_excel('processed.xlsx', index=False)
            except Exception as x:
                path = pathlib.Path('logs_errors.txt')
                print('Adding exception to error logs - ' + str(path.absolute()) + str(x))

                try:
                    errors = open('logs_errors.txt', 'a')
                    ts = str(datetime.datetime.now())
                    errors.write(ts + ': ' + str(x) + '\n')
                    errors.close()
                except Exception as error_logs_exception:
                    print(error_logs_exception)
                    exit() 

        condition = 0

        # Retrieve vehicle, account_links, account_name, and transaction count from the db
        db_vehicles = check_query(vehicle)

        # Check if there is duplicate vehicles
        if len(db_vehicles) > 1:
            
            # Set up environment
            delete_vehicles = []
            delete_acc_vehicles = []
            inactive_ids = []
            cashacc = 0
            accacc = 0
            mulacc = 0
            othacc = 0
            acacc = 0
            ocacc = 0
            
            # Get vehicle features
            cash_count = len(db_vehicles.loc[(db_vehicles['acc_id'].isin([1])) & db_vehicles['acc_id'].notna()])
            acc_count = len(db_vehicles.loc[(db_vehicles['acc_id'].isin([account_id])) & db_vehicles['acc_id'].notna()])
            oth_count = len(db_vehicles.loc[~(db_vehicles['acc_id'].isin([1,account_id])) & db_vehicles['acc_id'].notna()])
            if (cash_count > 0) & (acc_count > 0) & (oth_count > 0):
                mulacc = 1
            elif (cash_count > 0) & (acc_count > 0) & (oth_count == 0):
                acacc = 1
            elif (cash_count > 0) & (acc_count == 0) & (oth_count == 0):
                cashacc = 1
            elif (cash_count == 0) & (acc_count > 0) & (oth_count == 0):
                accacc = 1
            elif (cash_count == 0) & (acc_count == 0) & (oth_count > 0):
                othacc = 1
            elif (cash_count == 0) & (acc_count > 0) & (oth_count > 0):
                ocacc = 1
            transaction_count = len(db_vehicles[db_vehicles['total'].notna()])
            
            
            # Check if vehicle have transactions
            if transaction_count == 0:
                # Vehicle with no transactions
                # Set condition
                if cashacc == 1:
                    condition = 4
                if accacc == 1:
                    condition = 7
                if mulacc == 1:
                    condition = 10
                if othacc == 1:
                    condition = 18
                if acacc == 1:
                    condition = 20
                if ocacc == 1:
                    condition = 27
                
                print('Condition {} - {}: {}'.format(str(condition), conditions.iloc[[condition - 1]]['condition'][condition], conditions.iloc[[condition - 1]]['condition_description'][condition]))
                
                ## Log text file
                ts = str(datetime.datetime.now())
                logs.write('{}: {} with condition {} - {}: {}\n'.format(ts, vehicle, str(condition), conditions.iloc[[condition - 1]]['condition'][condition], conditions.iloc[[condition - 1]]['condition_description'][condition])) 
                
                # Get IDs of oldest vehicle
                vehicle_id = db_vehicles['id'][0]
                a_vehicle_id = db_vehicles['acc_veh_id'].iloc[0]
                
                # Relink oldest vehicle if not already linked
                if condition != 7:
                    move_link(veh_acc_id=a_vehicle_id, acco_id=account_id)
                    
                    # Get updated records
                    db_vehicles = check_query(vehicle)
                
                # Get IDs of vehicles to delete
                delete_vehicles = list(map(int, db_vehicles[(db_vehicles['id'] != vehicle_id)]['id'].tolist()))
                delete_acc_vehicles = list(map(int, db_vehicles[(db_vehicles['acc_veh_id'] != a_vehicle_id) & db_vehicles['acc_veh_id'].notna()]['acc_veh_id'].tolist()))

                # Delete duplicate vehicles
                delete_records(acc_veh_ids=delete_acc_vehicles, veh_ids=delete_vehicles)
                
                # Check if active
                check_active(vehicle_id=vehicle_id)
                
                # Add processed
                add_processed(condition=condition, active_id=vehicle_id)
                
            else:
                # Vehicle with transactions
                
                # Get transaction counts per account
                transactions_on_cash = len(db_vehicles[(db_vehicles['acc_id'] == 1) & db_vehicles['total'].notna()])
                transactions_on_acc = len(db_vehicles[(db_vehicles['acc_id'] == account_id) & db_vehicles['total'].notna()])
                transactions_on_oth = len(db_vehicles[~(db_vehicles['acc_id'].isin([1,account_id])) & db_vehicles['total'].notna()])
                
                # Set up environment
                ctx = 0
                cmtx = 0
                atx = 0
                amtx = 0
                mtx = 0
                mmtx = 0
                otx = 0
                xtx = 0
                
                if (transactions_on_cash == 1) & (transactions_on_acc == 0) & (transactions_on_oth == 0):
                    ctx = 1
                elif (transactions_on_cash > 1) & (transactions_on_acc == 0) & (transactions_on_oth == 0):
                    cmtx = 1
                elif (transactions_on_cash == 0) & (transactions_on_acc == 1) & (transactions_on_oth == 0):
                    atx = 1
                elif (transactions_on_cash == 0) & (transactions_on_acc > 1) & (transactions_on_oth == 0):
                    amtx = 1
                elif (transactions_on_cash == 1) & (transactions_on_acc == 1) & (transactions_on_oth == 0):
                    mtx = 1
                elif (transactions_on_cash == 1 or transactions_on_cash > 1) & (transactions_on_acc == 1 or transactions_on_acc > 1) & (transactions_on_oth == 0):
                    mmtx = 1
                elif (transactions_on_oth > 0):
                    otx = 1
                
                # Set conditions
                if (ctx == 1) & (cashacc == 1):
                    condition = 5
                elif (ctx == 1) & (mulacc == 1):
                    condition = 13
                elif (ctx == 1) & (acacc == 1):
                    condition = 23
                elif (cmtx == 1) & (cashacc == 1):
                    condition = 6
                elif (cmtx == 1) & (mulacc == 1):
                    condition = 14
                elif (cmtx == 1) & (acacc == 1):
                    condition = 24
                elif (atx == 1) & (accacc == 1):
                    condition = 8
                elif (atx == 1) & (mulacc == 1):
                    condition = 11
                elif (atx == 1) & (acacc == 1):
                    condition = 21
                elif (amtx == 1) & (accacc == 1):
                    condition = 9
                elif (amtx == 1) & (mulacc == 1):
                    condition = 12
                elif (amtx == 1) & (acacc == 1):
                    condition = 22
                elif (mtx == 1) & (mulacc == 1):
                    condition = 15
                elif (mmtx == 1) & (mulacc == 1):
                    condition = 16
                elif (mtx == 1) & (acacc == 1):
                    condition = 25
                elif (mmtx == 1) & (acacc == 1):
                    condition = 26
                elif (otx == 1) & (mulacc == 1):
                    condition = 17
                elif (otx == 1) & (othacc == 1):
                    condition = 19
                
                if condition in [15,16,25,26]:
                    # Cleaning account & cash transaction vehicles
                    t_moved = 0
                    print('Condition {} - {}: {}'.format(str(condition), conditions.iloc[[condition - 1]]['condition'][condition], conditions.iloc[[condition - 1]]['condition_description'][condition]))
                
                    ## Log text file
                    ts = str(datetime.datetime.now())
                    logs.write('{}: {} with condition {} - {}: {}\n'.format(ts, vehicle, str(condition), conditions.iloc[[condition - 1]]['condition'][condition], conditions.iloc[[condition - 1]]['condition_description'][condition]))
                    

                    # Move transactions from other vehicles to one with most transactions
                    a_vehicle_id = int(db_vehicles[(db_vehicles['acc_id'] == account_id) & db_vehicles['total'].notna()]['acc_veh_id'].iloc[0])
                    inactive_ids = list(map(int, db_vehicles[(db_vehicles['acc_veh_id'] != a_vehicle_id) & db_vehicles['acc_veh_id'].notna()]['acc_veh_id'].tolist()))

                    t_moved = move_transactions(inactive_ids=inactive_ids, active_id=a_vehicle_id)

                    # Get latest data from db
                    db_vehicles = check_query(vehicle)

                    # Get ID of vehicle with transactions
                    df = db_vehicles[db_vehicles['total'].notna()]
                    a_vehicle_id = df['acc_veh_id'].iloc[0]
                    vehicle_id = df['id'].iloc[0]

                    # Get IDs of vehicles to delete
                    delete_vehicles = list(map(int, db_vehicles[(db_vehicles['id'] != vehicle_id)]['id'].tolist()))
                    delete_acc_vehicles = list(map(int, db_vehicles[(db_vehicles['acc_veh_id'] != a_vehicle_id) & db_vehicles['acc_veh_id'].notna()]['acc_veh_id'].tolist()))

                    # Delete duplicate vehicles
                    delete_records(acc_veh_ids=delete_acc_vehicles, veh_ids=delete_vehicles)

                    # Check if active
                    check_active(vehicle_id=vehicle_id)

                    # Add processed
                    add_processed(condition=condition, active_id=vehicle_id, t_moved=t_moved)
                    
                elif condition in [8,9,11,12,21,22]:
                    # Cleaning account-transaction-only vehicles
                    t_moved = 0
                    print('Condition {} - {}: {}'.format(str(condition), conditions.iloc[[condition - 1]]['condition'][condition], conditions.iloc[[condition - 1]]['condition_description'][condition]))
                
                    ## Log text file
                    ts = str(datetime.datetime.now())
                    logs.write('{}: {} with condition {} - {}: {}\n'.format(ts, vehicle, str(condition), conditions.iloc[[condition - 1]]['condition'][condition], conditions.iloc[[condition - 1]]['condition_description'][condition]))
                    
                    if condition in [9,12,22]:
                        # Move transactions from other vehicles to one with most transactions
                        a_vehicle_id = int(db_vehicles[(db_vehicles['acc_id'] == account_id) & db_vehicles['total'].notna()]['acc_veh_id'].iloc[0])
                        inactive_ids = list(map(int, db_vehicles[(db_vehicles['acc_veh_id'] != a_vehicle_id) & db_vehicles['acc_veh_id'].notna()]['acc_veh_id'].tolist()))
                        
                        t_moved = move_transactions(inactive_ids=inactive_ids, active_id=a_vehicle_id)
                        
                        # Get latest data from db
                        db_vehicles = check_query(vehicle)
                        
                    # Get ID of vehicle with transactions
                    df = db_vehicles[db_vehicles['total'].notna()]
                    a_vehicle_id = df['acc_veh_id'].iloc[0]
                    vehicle_id = df['id'].iloc[0]

                    # Get IDs of vehicles to delete
                    delete_vehicles = list(map(int, db_vehicles[(db_vehicles['id'] != vehicle_id)]['id'].tolist()))
                    delete_acc_vehicles = list(map(int, db_vehicles[(db_vehicles['acc_veh_id'] != a_vehicle_id) & db_vehicles['acc_veh_id'].notna()]['acc_veh_id'].tolist()))

                    # Delete duplicate vehicles
                    delete_records(acc_veh_ids=delete_acc_vehicles, veh_ids=delete_vehicles)

                    # Check if active
                    check_active(vehicle_id=vehicle_id)

                    # Add processed
                    add_processed(condition=condition, active_id=vehicle_id, t_moved=t_moved)
                    
                elif condition in [5,6,13,14,23,24]:
                    # CLeaning Cash-transaction-only vehicles
                    t_moved = 0
                    print('Condition {} - {}: {}'.format(str(condition), conditions.iloc[[condition - 1]]['condition'][condition], conditions.iloc[[condition - 1]]['condition_description'][condition]))
                
                    ## Log text file
                    ts = str(datetime.datetime.now())
                    logs.write('{}: {} with condition {} - {}: {}\n'.format(ts, vehicle, str(condition), conditions.iloc[[condition - 1]]['condition'][condition], conditions.iloc[[condition - 1]]['condition_description'][condition])) 
                    
                    if condition in [6,14,24]:
                        # Move transactions from other vehicles to one with most transactions
                        a_vehicle_id = int(db_vehicles[(db_vehicles['acc_id'] == 1) & db_vehicles['total'].notna()]['acc_veh_id'].iloc[0])
                        inactive_ids = list(map(int, db_vehicles[(db_vehicles['acc_veh_id'] != a_vehicle_id) & db_vehicles['acc_veh_id'].notna()]['acc_veh_id'].tolist()))
                        
                        t_moved = move_transactions(inactive_ids=inactive_ids, active_id=a_vehicle_id)
                        
                        # Get latest data from db
                        db_vehicles = check_query(vehicle)
                        
                    # Get ID of vehicle with transactions
                    df = db_vehicles[db_vehicles['total'].notna()]
                    a_vehicle_id = df['acc_veh_id'].iloc[0]
                    vehicle_id = df['id'].iloc[0]

                    # Relink vehicle with transactions to correct account
                    move_link(veh_acc_id=a_vehicle_id, acco_id=account_id)

                    # Get updated records
                    db_vehicles = check_query(vehicle)

                    # Get IDs of vehicles to delete
                    delete_vehicles = list(map(int, db_vehicles[(db_vehicles['id'] != vehicle_id)]['id'].tolist()))
                    delete_acc_vehicles = list(map(int, db_vehicles[(db_vehicles['acc_veh_id'] != a_vehicle_id) & db_vehicles['acc_veh_id'].notna()]['acc_veh_id'].tolist()))

                    # Delete duplicate vehicles
                    delete_records(acc_veh_ids=delete_acc_vehicles, veh_ids=delete_vehicles)

                    # Check if active
                    check_active(vehicle_id=vehicle_id)

                    # Add processed
                    add_processed(condition=condition, active_id=vehicle_id, t_moved=t_moved)
                    
                elif condition in [17,19]:
                    # Cleaning other-transaction-only vehicles
                    print('Condition {} - {}: {}'.format(str(condition), conditions.iloc[[condition - 1]]['condition'][condition], conditions.iloc[[condition - 1]]['condition_description'][condition]))
                    
                    ## Log text file
                    ts = str(datetime.datetime.now())
                    logs.write('{}: {} with condition {} - {}: {}\n'.format(ts, vehicle, str(condition), conditions.iloc[[condition - 1]]['condition'][condition], conditions.iloc[[condition - 1]]['condition_description'][condition]))
        
                    #if condition.isin([17]):
                    #   # Move transactions from other vehicles to one with most transactions
                    #   a_vehicle_id = int(db_vehicles[~(db_vehicles['acc_id'].isin([1,account_id]) & db_vehicles['total'].notna()]['acc_veh_id'].iloc[0])
                    #   inactive_ids = list(map(int, db_vehicles[(db_vehicles['acc_veh_id'] != a_vehicle_id) & db_vehicles['acc_veh_id'].notna()]['acc_veh_id'].tolist()))
                        
                    #   t_moved = move_transactions(inactive_ids=inactive_ids, active_id=a_vehicle_id)
                        
                        # Get latest data from db
                    #   db_vehicles = check_query(vehicle)
                        
                    # Get IDs of vehicle with transactions
                    df = db_vehicles[db_vehicles['total'].notna()]
                    #a_vehicle_id = df['acc_veh_id'].iloc[0]
                    vehicle_id = df['id'].iloc[0]

                    # Relink oldest vehicle if not already linked
                    #move_link(veh_acc_id=a_vehicle_id, acco_id=account_id)

                    # Get updated records
                    #db_vehicles = check_query(vehicle)

                    # Get IDs of vehicles to delete
                    #delete_vehicles = list(map(int, db_vehicles[(db_vehicles['id'] != vehicle_id)]['id'].tolist()))
                    #delete_acc_vehicles = list(map(int, db_vehicles[(db_vehicles['acc_veh_id'] != a_vehicle_id) & db_vehicles['acc_veh_id'].notna()]['acc_veh_id'].tolist()))

                    # Delete duplicate vehicles
                    #delete_records(acc_veh_ids=delete_acc_vehicles, veh_ids=delete_vehicles)

                    # Check if active
                    #check_active(vehicle_id=vehicle_id)

                    # Add processed
                    add_processed(condition=condition, active_id=vehicle_id) #, t_moved=t_moved)
                
                else:
                    print('Unknown condition for {}.'.format(vehicle))
                    
        # Check if the only vehicle record found is correctly linked        
        elif len(db_vehicles) == 1:
            # Check if the single vehicle found is correctly linked
            if db_vehicles['acc_id'][0] == account_id:
                condition = 2
                print('Condition {} - {}: {}'.format(str(condition), conditions.iloc[[condition - 1]]['condition'][condition], conditions.iloc[[condition - 1]]['condition_description'][condition]))
                
                ## Log text file
                ts = str(datetime.datetime.now())
                logs.write('{}: {} with condition {} - {}: {}\n'.format(ts, vehicle, str(condition), conditions.iloc[[condition - 1]]['condition'][condition], conditions.iloc[[condition - 1]]['condition_description'][condition]))   
                
                # Set the account vehicle ID
                vehicle_id = db_vehicles['id'].iloc[0]

                # Check if active
                check_active(vehicle_id)
                
                # Add to processed
                add_processed(condition=condition, active_id=vehicle_id)

            # Check if single vehicle is linked to cash account
            elif db_vehicles['acc_id'][0] == 1:
                condition = 1
                print('Condition {} - {}: {}'.format(str(condition), conditions.iloc[[condition - 1]]['condition'][condition], conditions.iloc[[condition - 1]]['condition_description'][condition]))
                
                
                ## Log text file
                ts = str(datetime.datetime.now())
                logs.write('{}: {} with condition {} - {}: {}\n'.format(ts, vehicle, str(condition), conditions.iloc[[condition - 1]]['condition'][condition], conditions.iloc[[condition - 1]]['condition_description'][condition]))
                    
                # Move the vehicle to correct account
                a_vehicle_id = db_vehicles['acc_veh_id'].iloc[0]
                move_link(veh_acc_id=a_vehicle_id, acco_id=account_id)
                
                # Check whether the vehicle is active
                vehicle_id = db_vehicles['id'].iloc[0]
                check_active(vehicle_id)
                
                # Add to processed
                add_processed(condition=condition, active_id=vehicle_id)

            else:
                condition = 3
                print('Condition {} - {}: {}'.format(str(condition), conditions.iloc[[condition - 1]]['condition'][condition], conditions.iloc[[condition - 1]]['condition_description'][condition]))
                
                ## Log text file
                ts = str(datetime.datetime.now())
                logs.write('{}: {} with condition {} - {}: {}\n'.format(ts, vehicle, str(condition), conditions.iloc[[condition - 1]]['condition'][condition], conditions.iloc[[condition - 1]]['condition_description'][condition]))
                
                # Set the account vehicle ID
                a_vehicle_id = db_vehicles['acc_veh_id'].iloc[0]
                
                # Add to processed
                add_processed(condition=condition, active_id=a_vehicle_id)


        # Add vehicle to list of processed as not on the system
        else:
            print('Condition {} - Vehicle not loaded on the system'.format(str(condition)))
                
            ## Log text file
            ts = str(datetime.datetime.now())
            logs.write('{}: {} with condition {} - Vehicle not loaded on the system\n'.format(ts, vehicle, str(condition)))
            
            # Add vehicle to the system
            add_vehicle(vehicle=vehicle, accountid=account_id)
            
            # Add to processed list
            add_processed(condition=condition)


    # The main part of the program
    # Import vehicles from spreadsheet to check
    importfile = pathlib.Path('2process.xlsx')
    processdfile = pathlib.Path('processed.pickle')
    conditionfile = pathlib.Path('conditions.pickle')
    if processdfile.exists() & conditionfile.exists():
        if importfile.exists():
            vehicles_import = pd.read_excel(importfile, converters={'vehicle':str,'account_id':int})
            processed = pd.read_pickle(processdfile)
            conditions = pd.read_pickle(conditionfile)
            
            logs = open('logs.txt', 'a')
            ts = str(datetime.datetime.now())
            logs.write('----------------------------------------------------------------------------\n')
            logs.write(ts + ': Initiating data cleaning operation\n')
            logs.close()

            print("Initiating data cleaning operation")
            for i in range(0, len(vehicles_import)):
                vehicle = vehicles_import['vehicle'][i].replace(" ", "")
                account_id = vehicles_import['account_id'][i]
                print('----------------------------------------------------------------------------')
                print('Inspecting: ' + vehicle + ' (Number {} /{})'.format(str(i), str(len(vehicles_import)-1)))
                print('----------------------------------------------------------------------------')
                logs = open('logs.txt', 'a')
                ts = str(datetime.datetime.now())
                logs.write('----------------------------------------------------------------------------\n')
                logs.write('Processing: ' + str(vehicle) + ' for account ID: ' + str(account_id) + '\n')
                logs.write('----------------------------------------------------------------------------\n')
                if len(processed[processed['vehicle_registration'].isin([vehicle])]) > 0:
                    print('Already processed! Next.')
                    logs.write('Already processed! Next.\n')
                else:
                    check_condition(vehicle)
                print('\n')
                logs.write('\n')
                logs.close()
            print('-----------------Fleet list completed-------------------------')
        else:
            print('No files to process')
            return
    else:
        print('no files to process and records of processed! Creating new one')
        processed = pd.DataFrame(pd.DataFrame(columns=['vehicle_registration',
        'acc_no'
        'active_id', 
        'duplicate_veh_count',
        'duplicate_acc_veh_count', 
        'transactions_on_cash', 
        'transactions_on_account',
        'transactions_moved',
        'condition']))
        try:
            processed.to_pickle('processed.pickle')
            print('Rerun the program.')
        except Exception as x:
            print('Failed creating file: {}'.format(x))
            try:
                errors = open('logs_errors.txt', 'a')
                ts = str(datetime.datetime.now())
                errors.write(ts + ': ' + str(x) + '\n')
                errors.close()
            except Exception as error_logs_exception:
                print(error_logs_exception)
                exit()

# Call the main function to execute program
main()        