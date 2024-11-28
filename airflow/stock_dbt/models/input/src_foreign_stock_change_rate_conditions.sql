WITH src_foreign_stock_info_complete AS ( 
 SELECT * FROM {{ source ("raw", "fs_st_if_cpl") }}
)
SELECT NAME
     , FULLNAME 
     , DATE 
     , "Close" 
 FROM src_foreign_stock_info_complete 
