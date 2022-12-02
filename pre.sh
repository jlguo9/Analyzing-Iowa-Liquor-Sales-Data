#pre 
#download raw data
nohup wget https://data.iowa.gov/api/views/ykb6-ywnd/rows.csv?accessType=DOWNLOAD -O store.csv &

nohup wget https://data.iowa.gov/api/views/gckp-fe7r/rows.csv?accessType=DOWNLOAD -O product.csv &

nohup wget https://data.iowa.gov/api/views/m3tr-qhgy/rows.csv?accessType=DOWNLOAD -O iowaliquor.csv &


#date fix
sed -E "s#([0-9]{2})/([0-9]{2})/([0-9]{4})#\3-\1-\2#" < iowaliquor.csv | 
  tr -d '$' > iowa-liquor-datefixed.csv

sed -E "s#([0-9]{2})/([0-9]{2})/([0-9]{4})#\3-\1-\2#" < store.csv | 
  tr -d '$' > store-datefixed.csv

sed -E "s#([0-9]{2})/([0-9]{2})/([0-9]{4})#\3-\1-\2#" < product.csv | 
  tr -d '$' > product-datefixed1.csv

sed -E "s#([0-9]{2})/([0-9]{2})/([0-9]{4})#\3-\1-\2#" < product-datefixed1.csv | 
  tr -d '$' > product-datefixed.csv
