import pandas as pd


def transformation():
    data = pd.read_csv(r'home/oris/airflow/zipco_dag/zipco_transaction.csv')

    #Changing data datatype

    data['Date']= pd.to_datetime(data['Date'])

    # Remove duplicates
    data.drop_duplicates(inplace=True)

    # Handling missing values (Example: fill missing numeric values with the mean or median)
    numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_columns:
        data.fillna({col: data[col].mean()}, inplace=True)

    # Handling missing values (Example: fill missing string values with 'Unknown')
    string_columns = data.select_dtypes(include=['object']).columns
    for col in string_columns:
       data.fillna({col: 'Unknown'}, inplace=True)

    #Creating products table dataframes
    products= data[['ProductName']].drop_duplicates().reset_index(drop=True)
    products['ProductID']= range(1,len(products)+1)
    products= products[['ProductID','ProductName']]

    #Creating customers table dataframe
    customers= data[['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber',
        'CustomerEmail']].drop_duplicates().reset_index(drop=True)
    customers['CustomerID']= range(1, len(customers)+1)
    customers= customers[['CustomerID', 'CustomerName', 'CustomerAddress', 'Customer_PhoneNumber',
        'CustomerEmail']]

    #Creating staff table dataframe
    staff= data[['Staff_Name', 'Staff_Email']].drop_duplicates().reset_index(drop=True)
    staff['StaffID']= range(1, len(staff)+1)
    staff= staff[['StaffID','Staff_Name', 'Staff_Email']]

   #Creating fact table
    fact=data.merge(products, on=['ProductName'], how='left') \
            .merge(customers, on=['CustomerName','CustomerAddress', 'Customer_PhoneNumber','CustomerEmail'], how='left') \
            .merge(staff, on=['Staff_Name', 'Staff_Email'], how='left')

    fact['TransactionID']=range(1, len(fact)+1)
    fact= fact.reset_index()[['TransactionID','ProductID','CustomerID','StaffID','Date','Quantity', 'UnitPrice', 'StoreLocation',
        'PaymentType', 'PromotionApplied', 'Weather', 'Temperature','StaffPerformanceRating','CustomerFeedback', 'DeliveryTime_min',
        'OrderType','DayOfWeek','TotalSales']]
    
   #saving into csvfile
    data.to_csv('home/oris/airflow/zipco_dag/clean_data.csv', index=False)
    products.to_csv('home/oris/airflow/zipco_dag/products.csv', index=False)
    customers.to_csv('home/oris/airflow/zipco_dag/customers.csv', index=False)
    staff.to_csv('home/oris/airflow/zipco_dag/staff.csv', index=False)
    fact.to_csv('home/oris/airflow/zipco_dag/fact.csv', index=False)
    print('Normalised data saved successfully!')

