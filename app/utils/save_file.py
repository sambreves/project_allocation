def save_local_file_csv(file, name):
    path_local = f'./data/files/{name}.csv'
    
    try:
        file.to_csv(path_local, sep=';', decimal=',', index=False)
        print(f'Save: {path_local}')
    except (IndexError, TypeError) as error:
        print('Error:', error)