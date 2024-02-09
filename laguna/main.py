from  src.etl.api_extract import api_check , bucket_check
 

def main():
    url = 'https://api.escuelajs.co/api/v1/products'
    bucket_name = 'laguna-certification-associate'
    

    api_check(url)
    bucket_check()
    print('add repo')
    


if __name__ == '__main__':
    main()
    
    
    
    
    