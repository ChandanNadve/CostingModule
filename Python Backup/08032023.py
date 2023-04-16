# %% [markdown]
# # PROJECT NAME : COSTING MODULE

# %% [markdown]
# ## CHAPTER 1 BOM PROCESSING

# %% [markdown]
# IMPORT THE REQUIRED MODULES

# %%
import datetime
import sqlqueries as query
import pandas as pd
import os
import time
import warnings
import tkinter
import numpy as np
from pandas.core.common import SettingWithCopyWarning
import logging

warnings.simplefilter(action="ignore")

now=datetime.datetime.now()
f_time = now.strftime("%Y%m%d_%H%M%S")
folder_name = "Costing_Process_"+f_time
pd.pandas.set_option('display.max_columns',None)

os.mkdir(folder_name)
logfile=open(folder_name+"/ExecutionTimeLogFile.txt","w+")
processlogfile=open(folder_name+"/ProcessLogFile.txt","w+")

# %% [markdown]
# FUNCTION TO  READ ORACLE QUERY

# %%
starttime=datetime.datetime.now()
logfile.write(f'The Costing Process is Started {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')

# %%
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
engine = sqlalchemy.create_engine("oracle+cx_oracle://majan_uat:log@192.168.0.13/?service_name=orcl", arraysize=1000)
#engine = sqlalchemy.create_engine("oracle+cx_oracle://majan01032023:log@localhost/?service_name=orcl.majanglass.com", arraysize=1000)
def readoraclequery(query):
   try:
      orders_sql = query
      df_data = pd.read_sql(orders_sql, engine)
      return df_data
   except SQLAlchemyError as e:
      print(e)

# %% [markdown]
# PROCESSING COSTING MASTER TABLE IMPORT

# %%
process_costing_data =readoraclequery(query.costing_process_query) #7 Costing Processing Master
# ASSIGN THE VARIABLE
v_pricetype = process_costing_data.loc[0,'pricetype']
v_efficiencytype = process_costing_data.loc[0,'efficiencytype']
v_rawmaterialbomtype = process_costing_data.loc[0,'rawmaterialbomtype']
v_packingbomtype = process_costing_data.loc[0,'packingbomtype']
v_dispatchtype = process_costing_data.loc[0,'dispatchtype']
v_colorstype = process_costing_data.loc[0,'colorstype']
v_standardeference = process_costing_data.loc[0,'standardeference']
v_coatingtype = process_costing_data.loc[0,'coatingtype']
v_screeningtype = process_costing_data.loc[0,'screeningtype']
v_fromdate = np.datetime64(process_costing_data.loc[0,'fromdate'])
v_todate = np.datetime64(process_costing_data.loc[0,'todate'])

# %% [markdown]
# MASTER TABLES PREPARATIONS

# %%
# Finished Goods Item Master and Item Master and UOM
fg_data = readoraclequery(query.fg_query) #1 FG Master
fg_data.rename(columns={'bottleconfiguration':'Bottle Configuration'},inplace=True)
uom_data=readoraclequery(query.uom_query) #11 UOM Query
fg_data_exp=fg_data.drop(columns=['finpmbasicid','pmbasicid'])
currency_data=readoraclequery(query.currency_query)

# Item Master
item_master_data=readoraclequery(query.item_master_query) #8 Item Master
item_master_data=item_master_data.merge(uom_data,left_on='primaryunit',right_on='uomid',how='inner')
item_master_data.drop(columns=['primaryunit','uomid'],inplace=True)
item_master_data.rename(columns={'unitcode':'Primary Unit'},inplace=True)

# %% [markdown]
# Color Furnace and Line Master

# %%
color_master_data=readoraclequery(query.color_query ) #4 Color Master
furnace_master_data=readoraclequery(query.furnace_query ) #5 Furnace Master
line_master_data=readoraclequery(query.linemaster_query) # 26 Line Master Query 

# %% [markdown]
# Processing , Selected Items and Reference Master

# %%
reference_name_data=readoraclequery(query.reference_name_query) # 14. Reference Name Query
selecteditem_data=readoraclequery(query.costing_process_query) # 25. Selected Items for Costing

# %% [markdown]
# Standard Efficiency

# %%
furnace_std_eff_data =readoraclequery(query.std_eff_query) #6 Standard Efficiency

# %% [markdown]
# Standard Rates for BOM

# %%
standard_rate_header_data=readoraclequery(query.standard_rate_header_query) #9 Standard Rate Header Data
standard_rate_details_data=readoraclequery(query.standard_rate_details_query) #10 Standard Rate Grid Data
standard_rate_header_data=standard_rate_header_data[standard_rate_header_data['name']==v_pricetype]
standard_rate_header_data=standard_rate_header_data[standard_rate_header_data['appdate']<=v_todate]

standard_rate_data = pd.merge(standard_rate_header_data,standard_rate_details_data,on=['stdprice_hdrid'],how='inner')
standard_rate_data.rename(columns={'itemid':'rmitemid'},inplace=True)

standard_rate_data_max=standard_rate_data.groupby(['rmitemid'])['appdate'].max().reset_index()
standard_rate_data=pd.merge(standard_rate_data,standard_rate_data_max,on=('appdate','rmitemid'))

stdr_uom_data=uom_data
stdr_uom_data.rename(columns={'unitcode':'Consumption Unit'},inplace=True)

# %% [markdown]
# Linking the Purchase UOM and Purchase Currency Master

# %%
standard_rate_data=standard_rate_data.merge(stdr_uom_data,left_on='consumptionunit',right_on='uomid',how='inner')
standard_rate_data=standard_rate_data.merge(currency_data,left_on='purcurrency',right_on='currencyid',how='inner')

item_master_data=standard_rate_data.merge(item_master_data,left_on='rmitemid',right_on='pmbasicid',how='right')

# %%
item_master_data=item_master_data[['pmbasicid','productgroup', 'productid', 'profitcode', 'productdesc', 'Primary Unit',  'Consumption Unit',  
                   'perqty', 'rate','conversionfactor',  'priceinuom','exrate',  'currency',
                     'procategcode', 'procategory']]

# %%
item_master_data.rename(columns={'productgroup':'Product Group', 
                                 'productid':'Item Code', 
                                 'profitcode':'Profit Code',
                                 'productdesc':'Product Desc', 
                                 'perqty':'Per Qty', 
                                 'rate':'Rate',
                                 'conversionfactor':'Conversion Factor',  
                                 'priceinuom':'Price in UOM',
                                 'exrate':'Ex Rate',  
                                 'currency':'Currency',
                                 'procategcode':'Category Code', 
                                 'procategory':'Product Category'},inplace=True)

# %% [markdown]
# Actual Issue Rates

# %%
actual_rate_data=readoraclequery(query.issue_query)
actual_rate_data['year']=pd.to_datetime(actual_rate_data['docdt']).dt.year # Add the Year Column
actual_rate_data=actual_rate_data[actual_rate_data['docdt']<=v_todate]
actual_rate_data=actual_rate_data.groupby(['productid','year'])['amount','issueqty'].sum().reset_index()
actual_rate_data['Actual Rate']=actual_rate_data['amount']/actual_rate_data['issueqty']
actual_rate_data_max=actual_rate_data.groupby(['productid'])['year'].max().reset_index()
actual_rate_data=actual_rate_data.merge(actual_rate_data_max,on=['productid','year'],how='inner')
actual_rate_data.rename(columns={'year':'Consumption Year','issueqty':'Consumption Quantity','amount':'Consumption Amount'},inplace=True)
item_master_data=actual_rate_data.merge(item_master_data,left_on='productid',right_on='pmbasicid',how='right')

# %%
item_master_data=item_master_data[['pmbasicid','Product Group', 'Item Code', 'Profit Code', 'Product Desc',
       'Primary Unit', 'Consumption Unit', 'Per Qty', 'Rate','Conversion Factor', 'Price in UOM', 'Ex Rate', 'Currency',
       'Category Code', 'Product Category', 'Consumption Year', 'Consumption Amount', 'Consumption Quantity', 'Actual Rate']]

# %% [markdown]
# Efficiency Summary

# %%
efficiency_data=readoraclequery(query.efficiency_query)
cy_efficiency_data_detailed=efficiency_data[(efficiency_data['docdt']<=v_todate)  & (efficiency_data['docdt']>=v_fromdate)]

productwise_eff=cy_efficiency_data_detailed.groupby(['productid','productdesc'])['prbottles','packbottles','prodtonnage','packtonnage'].sum().reset_index()
product_linewise_eff=cy_efficiency_data_detailed.groupby(['productid','productdesc','linedesc'])['prbottles','packbottles','prodtonnage','packtonnage'].sum().reset_index()
linewise_eff=cy_efficiency_data_detailed.groupby(['linedesc'])['prbottles','packbottles','prodtonnage','packtonnage'].sum().reset_index()
product_linewise_eff['Efficiency']=product_linewise_eff['packtonnage']/product_linewise_eff['prodtonnage']*100
linewise_eff['Efficiency']=linewise_eff['packtonnage']/linewise_eff['prodtonnage']*100

product_linewise_eff.rename(columns={'productid':'Product ID', 'productdesc':'Product Desc', 'linedesc':'Line Desc', 
                                     'prbottles':'Product Bottles', 'packbottles':'Packed Bottles',
                                     'prodtonnage':'Production Tonnage', 'packtonnage':'Packed Tonnage'},inplace=True)

linewise_eff.rename(columns={'productid':'Product ID', 'productdesc':'Product Desc', 'linedesc':'Line Desc',
                             'prbottles':'Product Bottles', 'packbottles':'Packed Bottles',
                             'prodtonnage':'Production Tonnage', 'packtonnage':'Packed Tonnage'},inplace=True)

# %% [markdown]
# ### PACKING COSTING

# %% [markdown]
# ### FC1 FURNACE COSTING

# %% [markdown]
# Part A - Reading the Furnance Header and Grid Data and Merging

# %%
logfile.write(f'\nFurnace BOM started at {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')

fg_rm_bom_header_data = readoraclequery(query.rmbom_header_query) #2 RM Bill Of Material Header
fg_rm_bom_details_data = readoraclequery(query.rmbom_detail_query) #3 RM Bill Of Material Detail

fg_rm_bom_header_data=fg_rm_bom_header_data[fg_rm_bom_header_data['name']==v_rawmaterialbomtype]
fg_rm_bom_header_data_temp=fg_rm_bom_header_data.groupby(['color','furnance'])['appdate'].max().reset_index()
fg_rm_bom_header_data=pd.merge(fg_rm_bom_header_data,fg_rm_bom_header_data_temp,on=['color','furnance','appdate'],how='inner')
fg_rm_bom_full_data = pd.merge(fg_rm_bom_header_data,fg_rm_bom_details_data,on=['rmbomhdrid'],how='inner')


# %% [markdown]
# Part B - Join With RM BOM Data Grid

# %%
fg_rm_bom_full_data=fg_rm_bom_full_data[['color', 'furnance',  'rmitemname', 'rmitemcode', 
                                         'rmunitco','qtyperbatch', 'evloss', 'glassdraw', 'rmefficiency', 'rmnetpacked','culletqty']]

# %%
#Rename the Columns for Joining with color and furnace master and Join(Merge)
fg_rm_bom_full_data.rename(columns={'color':'colormasterid','furnance':'psysfurnaceid'},inplace=True)
fg_rm_bom_full_data=pd.merge(fg_rm_bom_full_data,color_master_data,on=['colormasterid'],how='inner')
fg_rm_bom_full_data=pd.merge(fg_rm_bom_full_data,furnace_master_data,on=['psysfurnaceid'],how='inner')

furnace_std_eff_data_temp=furnace_std_eff_data.groupby(['color','furnance'])['docdate'].max().reset_index()
furnace_std_eff_data=pd.merge(furnace_std_eff_data,furnace_std_eff_data_temp,on=['color','furnance','docdate'],how='inner')

furnace_std_eff_data.rename(columns={'color':'colormasterid','furnance':'psysfurnaceid'},inplace=True)
fg_rm_bom_full_data= pd.merge(fg_rm_bom_full_data,furnace_std_eff_data,on=['psysfurnaceid','colormasterid'],how='inner')

# %%
raw_material_items=item_master_data[item_master_data['Product Group']=='RAW MATERIALS'].reset_index()
raw_material_items[(raw_material_items['Primary Unit']=='MT') & (raw_material_items['Conversion Factor'].isna())]['Conversion Factor']=1000
raw_material_items.loc[((raw_material_items['Primary Unit']=='MT') & (raw_material_items['Conversion Factor'].isna())),'Conversion Factor']=1000

# %%

rm_item_data=item_master_data
fg_rm_bom_full_data=pd.merge(fg_rm_bom_full_data,rm_item_data,left_on='rmitemname' ,right_on='pmbasicid',how='inner')

rm_cons_uom_data=uom_data
rm_cons_uom_data.rename(columns={'uomid':'rmunitco','unitcode':'Consumption Unit'},inplace=True)

fg_rm_bom_full_data=pd.merge(fg_rm_bom_full_data,rm_cons_uom_data,on=['rmunitco'],how='inner')
fg_rm_bom_full_data['Amt/Batch']=fg_rm_bom_full_data['Price in UOM']*fg_rm_bom_full_data['qtyperbatch']

fg_rm_bom_full_data_exp=fg_rm_bom_full_data[['furnacename','colorname', 'Item Code','Product Desc','Primary Unit','Price in UOM','qtyperbatch',
                                             'Amt/Batch','evloss','glassdraw','efficiency','culletqty']]

fg_rm_bom_full_data_exp["Net Packed"]=fg_rm_bom_full_data_exp["efficiency"]*fg_rm_bom_full_data_exp["glassdraw"]/100

fg_rm_bom_full_data_exp=fg_rm_bom_full_data_exp.sort_values(by=['furnacename','colorname'],ascending=True)
fg_rm_bom_full_data_exp.rename(columns={'furnacename':'Furnace',
                                        'colorname':'Color',
                                        'productid':'Product ID',
                                        'productdesc':'Product Name',
                                        'priceinuom':'Rate in Cons.UOM',
                                        'unitcode':'Cons. Unit',
                                        'qtyperbatch':'Qty/Batch',
                                        'evloss':'Evoperation Loss',
                                        'glassdraw':'Glass Draw',
                                        'efficiency':'Efficiency',
                                       'culletqty':'Cullet Quantity'},inplace=True)

rm_culletrate=fg_rm_bom_full_data_exp.groupby(['Furnace','Color']).agg({'Amt/Batch':'sum','Glass Draw':'sum'}).reset_index()
rm_culletrate["Cullet Rate"]=rm_culletrate["Amt/Batch"]/rm_culletrate["Glass Draw"]
rm_culletrate.drop(columns=['Amt/Batch','Glass Draw'],inplace=True)

fg_rm_bom_full_data_exp=pd.merge(fg_rm_bom_full_data_exp,rm_culletrate,on=['Furnace','Color'],how='inner')
fg_rm_bom_full_data_exp["Cullet Amount"]=fg_rm_bom_full_data_exp['Cullet Quantity']*fg_rm_bom_full_data_exp["Cullet Rate"]
fg_rm_bom_full_data_exp["Amount Net"]=fg_rm_bom_full_data_exp['Cullet Amount']*fg_rm_bom_full_data_exp["Cullet Rate"]

fg_rm_bom_summary_data_exp=fg_rm_bom_full_data_exp.groupby(['Furnace','Color','Evoperation Loss', 'Efficiency'])['Qty/Batch', 'Amt/Batch', 'Glass Draw', 
                                                                                                                 'Cullet Quantity','Net Packed', 'Cullet Rate', 'Cullet Amount', 'Amount Net'].sum().reset_index()

# %% [markdown]
# ### PACKING COSTING

# %% [markdown]
# Fetching Header Data and Taking the latest Packing Cost BOM

# %%
logfile.write(f'\nPacking BOM started at {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
# Reading the Packing BOM Header and Details Tables
pkg_bom_header_data=readoraclequery(query.pkg_bom_header_query) # 12. BOM Packing Header Query
pkg_bom_detail_data=readoraclequery(query.pkg_bom_detail_query) # 13. BOM Packing Detail Query

# Taking the latest Packing BOM
pkg_bom_header_data_max=pkg_bom_header_data.groupby(['fgitemcode']).agg({'appdate':'max'}).reset_index()
tray_master_data=readoraclequery(query.tray_master_query)
packing_bom_code_data=readoraclequery(query.packing_bom_code_query)
pkg_bom_header_data=pkg_bom_header_data.fillna(0)

fg_pkg_bom_full_data=pd.merge(pkg_bom_header_data,pkg_bom_header_data_max,on='fgitemcode',how='inner')
fg_pkg_bom_full_data_exp=pd.merge(pkg_bom_header_data,pkg_bom_detail_data,on=['bompackmaterialhrdid'],how='inner')
fg_pkg_bom_full_data_exp.drop(columns=['bompackmaterialhrdid','appdate','name','bompackmaterialdtlid'],inplace=True)

# Joined with Finshed Goods and Packing Material Code
fg_data_pkg=fg_data[["finpmbasicid","Product Group","FG Code","FG Name","Qty/Tray","Tray/Pallet","Qty/Pallet"]]
fg_data_pkg.rename(columns={"Product Group":"FG Product Group"},inplace=True)

rm_data_pkg=item_master_data[['pmbasicid', 'Product Group', 'Item Code', 'Profit Code',
       'Product Desc', 'Primary Unit', 'Consumption Unit', 'Per Qty', 'Rate',
       'Conversion Factor', 'Price in UOM', 'Ex Rate', 'Currency',
       'Category Code', 'Product Category']]
fg_pkg_bom_full_data_exp=pd.merge(fg_pkg_bom_full_data_exp,fg_data_pkg,left_on=["fgitemcode"],right_on=["finpmbasicid"],how="inner")
fg_pkg_bom_full_data_exp=pd.merge(fg_pkg_bom_full_data_exp,rm_data_pkg,left_on=["matcode"],right_on=["pmbasicid"],how="inner")

fg_pkg_bom_full_data_exp=pd.merge(fg_pkg_bom_full_data_exp,packing_bom_code_data,left_on=["packingtype"],right_on=["csysbommasterid"],how="inner")
fg_pkg_bom_full_data_exp=pd.merge(fg_pkg_bom_full_data_exp,tray_master_data,left_on=["traycode"],right_on=["psystraymasterid"],how="inner")

# %%
fg_pkg_bom_full_data_exp=fg_pkg_bom_full_data_exp.fillna(0)
fg_pkg_bom_full_data_exp=fg_pkg_bom_full_data_exp[[ 'FG Code', 'FG Name','bom_code', 'bom_name','tray_code', 'tray_name',
       'Qty/Tray','FG Product Group',
       'bottbox', 'boxlayer', 'boxpallet', 'boxcount','bottlespercontainer','Qty/Tray',
       'Tray/Pallet', 'Qty/Pallet', 'Product Group',
       'Item Code', 'Profit Code', 'Product Desc', 'Primary Unit', 'Consumption Unit',
       'qtyco', 'qtycon','Rate']]

fg_pkg_bom_full_data_exp.rename(columns={'bom_code':'BOM Code','bom_name':'BOM Name','tray_code':'Tray Code',
                                         'tray_name':'Tray Name','bottbox':'Bottles/Box','boxlayer':'Box/Layer',
                                         'boxpallet':'Box/Pallet','boxcount':'Box/Count','qtyco':'Qty',
                                         'qtycon':'Qty/Container','bottlespercontainer':"Bottles/Container"},inplace=True)

fg_pkg_bom_full_data_exp['Amount']=(fg_pkg_bom_full_data_exp['Qty']*fg_pkg_bom_full_data_exp['Rate']/fg_pkg_bom_full_data_exp['Qty/Pallet'] )+fg_pkg_bom_full_data_exp['Qty/Container']*fg_pkg_bom_full_data_exp['Rate']/fg_pkg_bom_full_data_exp['Bottles/Container']

# %% [markdown]
# ### PACKING SUMMARY COSTING

# %%
fg_pkg_bom_summary_data=fg_pkg_bom_full_data_exp.groupby(['FG Code','FG Name'])['Amount'].sum().reset_index()

# %% [markdown]
# ### COLOR SCREENING COSTING

# %%
logfile.write(f'\nColor Screening BOM started at {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
screenhdr_data=readoraclequery(query.screenhdr_query) # 23.BOM Colors Screening Header Query
screendtl_data=readoraclequery(query.screendtl_query) # 24.BOM Colors Screening Detail Query

screendtl_data=screendtl_data[['bomcolorshdr_scrid',  'matcode', 'qtyco']]

screenhdr_data=screenhdr_data[screenhdr_data['name']==v_screeningtype]

screenhdr_data=screenhdr_data[screenhdr_data['appdate']<=v_todate]
screenhdr_data=screenhdr_data[['bomcolorshdr_scrid',  'fgitemcode']]

screen_full_data=pd.merge(screenhdr_data,screendtl_data,on=['bomcolorshdr_scrid'],how='inner')
screen_full_data_exp=screen_full_data

fg_data_clrscreen=item_master_data[['pmbasicid', 'Product Group', 'Item Code', 'Profit Code',
       'Product Desc', 'Primary Unit', 'Consumption Unit', 'Per Qty', 'Rate',
       'Conversion Factor', 'Price in UOM', 'Ex Rate', 'Currency',
       'Category Code', 'Product Category']]
screen_full_data_exp=pd.merge(screen_full_data_exp,fg_data_clrscreen,left_on='matcode',right_on='pmbasicid',how='inner')

screen_full_data_exp=screen_full_data_exp[[   'Item Code','Product Desc', 'Primary Unit', 'Consumption Unit','Per Qty', 'Rate', 'Conversion Factor', 'Price in UOM', 'Ex Rate',
       'Currency', 'Category Code', 'Product Category','qtyco']]
screen_full_data_exp['Cost/Screen']=screen_full_data_exp['Price in UOM']/screen_full_data_exp['qtyco']
screen_full_data_exp.rename(columns={'qtyco':'No.Of Screens'},inplace=True)

# %% [markdown]
# ### COLOR COSTING

# %%
logfile.write(f'\nColor BOM started at {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
bomcolorshdr_data=readoraclequery(query.bomcolorshdr_query) # 21.BOM Colors Header Query

bomcolorshdr_data=bomcolorshdr_data[bomcolorshdr_data['name']==v_colorstype]  #Consider the type of Costing
bomcolorshdr_data=bomcolorshdr_data[bomcolorshdr_data['appdate']<=v_todate]  #Consider he record which are all before todate
bomcolorshdr_data_max=bomcolorshdr_data.groupby('fgitemcode')['docdt'].max().reset_index()
bomcolorshdr_data=pd.merge(bomcolorshdr_data,bomcolorshdr_data_max,on=['fgitemcode','docdt'],how='inner')
bomcolorshdr_data=bomcolorshdr_data[['bomcolorshdrid', 'fgitemcode','stdbottqty']]

bomcolorsdtl_data=readoraclequery(query.bomcolorsdtl_query) # 22.BOM Colors Detail Query
bomcolorsdtl_data=bomcolorsdtl_data[['bomcolorshdrid', 'matcode','qtyco']]

color_full_data=pd.merge(bomcolorshdr_data,bomcolorsdtl_data,on=['bomcolorshdrid'],how='inner')
color_full_data_exp=color_full_data

fg_data_clr=fg_data[["finpmbasicid","FG Code","FG Name","Qty/Tray","Tray/Pallet","Qty/Pallet","Qty/Pallet"]]
rm_data_clr=item_master_data[['pmbasicid', 'Product Group', 'Item Code', 'Profit Code','Product Desc', 'Primary Unit','Consumption Unit','Price in UOM']]
color_full_data_exp=pd.merge(color_full_data_exp,fg_data_clr,left_on=["fgitemcode"],right_on=["finpmbasicid"],how="inner")
color_full_data_exp=pd.merge(color_full_data_exp,rm_data_clr,left_on=["matcode"],right_on=["pmbasicid"],how="inner")

color_full_data_exp=color_full_data_exp[['FG Code', 'FG Name','Item Code', 'Product Desc', 'Primary Unit','Consumption Unit','qtyco', 'Price in UOM','stdbottqty']]
color_full_data_exp.rename(columns={'qtyco':'Quantity','stdbottqty':'Per Bottles'},inplace=True)
color_full_data_exp['Amount']=color_full_data_exp['Quantity']*color_full_data_exp['Price in UOM']

color_full_data_exp['Rate/Bottle']=color_full_data_exp['Amount']/color_full_data_exp['Per Bottles']
color_summary_data_exp=color_full_data_exp.groupby(['FG Code', 'FG Name'])['Rate/Bottle'].sum().reset_index()

# %% [markdown]
# ### COATING BOM

# %%
logfile.write(f'\nCoating BOM started at {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
try:
    bomcoatinghdr_data=readoraclequery(query.bomcoatinghdr_query) # 19.BOM Coating Header Query
    bomcoatinghdr_data=bomcoatinghdr_data[['bomcoatinghdrid','appdate', 'name', 'line','linespeed']] # Removed Unwanted Columns in header table
    bomcoatinghdr_data=bomcoatinghdr_data[bomcoatinghdr_data['appdate']<=v_todate]  # Take the coating dat which is applicable is less than todat
    bomcoatinghdr_data=bomcoatinghdr_data[bomcoatinghdr_data['name']==(v_coatingtype)] # Take the selecledt Coating Type
    bomcoatinghdr_data=bomcoatinghdr_data[['bomcoatinghdrid','line','linespeed']]
    bomcoatingdtl_data=readoraclequery(query.bomcoatingdtl_query) # 20. BOM Coating Detail Query
    bomcoatingdtl_data=bomcoatingdtl_data[['bomcoatinghdrid', 'coatingitemname','coatingitemcode', 'coatingunitco', 'coatqty', 'coatremarks']]

    coating_full_data=pd.merge(bomcoatinghdr_data,bomcoatingdtl_data,on=['bomcoatinghdrid'],how='inner')
    coating_full_data_exp=coating_full_data[[ 'line','linespeed', 'coatingitemname', 'coatingitemcode','coatingunitco', 'coatqty']]
    rm_data_coat=item_master_data[['pmbasicid', 'Product Group', 'Item Code', 'Profit Code','Product Desc', 'Primary Unit',
                                    'Consumption Unit', 'Per Qty', 'Rate','Conversion Factor', 'Price in UOM', 
                                    'Ex Rate', 'Currency','Category Code', 'Product Category']]

    coating_full_data_exp=pd.merge(coating_full_data_exp,rm_data_coat,left_on='coatingitemname',right_on='pmbasicid',how='inner')
    coating_full_data_exp=pd.merge(coating_full_data_exp,line_master_data,left_on=["line"],right_on=["psyslinemasterid"],how="inner")

    coating_full_data_exp.rename(columns={'lineid':'Line ID', 'linedesc':'Line Desc','linespeed':'Line Speed'},inplace=True)

    coating_full_data_exp=coating_full_data_exp[['Product Group','Item Code','Product Desc', 'Primary Unit','Line ID', 'Line Desc','Line Speed', 'Consumption Unit', 'coatqty','Price in UOM']]
    coating_full_data_exp.rename(columns={'coatqty':'Quantity Cons'},inplace=True)
    coating_full_data_exp['Bottles/Day']=coating_full_data_exp['Line Speed']*24*60

    coating_full_data_exp['Cost/Day']=coating_full_data_exp['Quantity Cons']*coating_full_data_exp['Price in UOM']

    coating_full_data_exp['Cost/Bottle']=coating_full_data_exp['Cost/Day']/coating_full_data_exp['Bottles/Day']
    coating_summary_data_exp=coating_full_data_exp.groupby(['Line Desc'])['Cost/Bottle'].sum().reset_index()
except Exception as e:
    logfile.write(f'\nCoating BOM occured Error {e} at {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
    print(f'\nCoating BOM occured Error {e} at {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')

# %% [markdown]
# ### DISPATCH BOM

# %%
logfile.write(f'\nDispatch BOM started at {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
bomdespatchhdr_data=readoraclequery(query.bomdespatchhdr_query) # 19. BOM Dispatch Header Query
bomdespatchhdr_data=bomdespatchhdr_data[bomdespatchhdr_data['name']==v_dispatchtype]
bomdespatchhdr_data=bomdespatchhdr_data[bomdespatchhdr_data['appdate']<=v_todate]
bomdespatchhdr_data[bomdespatchhdr_data['appdate']==np.datetime64(bomdespatchhdr_data['appdate'].max())]
bomdespatchhdr_data=bomdespatchhdr_data[['bomdespatchhdrid','despatch']]

bomdespatchdtl_data=readoraclequery(query.bomdespatchdtl_query) # 20. BOM Dispatch Detail Query
bomdespatch_full_data=pd.merge(bomdespatchhdr_data,bomdespatchdtl_data,on=['bomdespatchhdrid'],how='inner')
bomdespatch_full_data_exp=bomdespatch_full_data

despatchcode_data=readoraclequery(query.despatchcode_query)
despatchcode_data.rename(columns={'desp_code':'Despatch Code','desp_name':'Despatch Name','desp_details':'Despatch Details'},inplace=True)
bomdespatch_full_data_exp=pd.merge(bomdespatch_full_data_exp,despatchcode_data,left_on='despatch',right_on='csystdespatchid',how='inner')

rm_data_des=item_master_data[['pmbasicid', 'Product Group', 'Item Code', 'Profit Code','Product Desc', 'Primary Unit','Consumption Unit','Price in UOM']]
bomdespatch_full_data_exp=pd.merge(bomdespatch_full_data_exp,rm_data_pkg,left_on=["itemname"],right_on=["pmbasicid"],how="inner")

bomdespatch_full_data_exp=bomdespatch_full_data_exp[['Despatch Code','Item Code', 'Product Desc', 'Primary Unit', 'qty','Price in UOM','Despatch Name']]

bomdespatch_full_data_exp['Amount']=bomdespatch_full_data_exp['qty']*bomdespatch_full_data_exp['Price in UOM']
bomdespatch_full_data_exp.rename(columns={'qty':'Quantity'},inplace='True')

# %% [markdown]
# ### LABORS BOM

# %%
logfile.write(f'\nLabour BOM started at {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
bom_labcost_hdr_data=readoraclequery(query.bom_labcost_hdr_query) # 15. BOM Cost for Labors Header Query
bom_labcost_dtl_data=readoraclequery(query.bom_labcost_dtl_query) # 16. BOM Cost for Labors detail Query
bom_labcost_hdr_data=bom_labcost_hdr_data[['bom_labcost_hdrid', 'docid', 'docdt','name' ,'appdate']]

# %%
logfile.write(f'\nLabour BOM started at {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
bom_labcost_hdr_data=readoraclequery(query.bom_labcost_hdr_query) # 15. BOM Cost for Labors Header Query
bom_labcost_dtl_data=readoraclequery(query.bom_labcost_dtl_query) # 16. BOM Cost for Labors detail Query
bom_labcost_hdr_data=bom_labcost_hdr_data[['bom_labcost_hdrid', 'docid', 'docdt','name' ,'appdate']]

bom_labcost_hdr_data=bom_labcost_hdr_data[bom_labcost_hdr_data['appdate']<= v_todate]
bom_labcost_hdr_data[bom_labcost_hdr_data['appdate']==np.datetime64(bom_labcost_hdr_data['appdate'].max())]
bom_labcost_full_data=pd.merge(bom_labcost_hdr_data,bom_labcost_dtl_data,on=['bom_labcost_hdrid'],how='inner')
bom_labcost_full_data_exp=bom_labcost_full_data

# %% [markdown]
# ### LINE PRODUCTWISE EFFICIENCY

# %% [markdown]
# ## 2. OVERHEAD CALCULATION

# %% [markdown]
# ACL Running Days

# %%
acl_running_days_data=readoraclequery(query.acl_running_days_query)
acl_running_days_data=acl_running_days_data[(acl_running_days_data['lastday']>=v_fromdate) & (acl_running_days_data['lastday']<=v_todate)]
acl_running_days_data=acl_running_days_data.rename(columns={'line':'Line','noofdays1':'No.Of Days','lastday':'Month'})

# %% [markdown]
# Gas and Power Data Split Up

# %%


# %% [markdown]
# Reading Accounts Table and Cost Center Table

# %%
logfile.write(f'\nOverhead Calculations are started at {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
accounts_txn_data=readoraclequery(query.accounts_txn_query)
cost_center_txn_data=readoraclequery(query.cost_center_txn_query)
cost_pool_data=readoraclequery(query.cost_pool_query)

# %% [markdown]
# Cost Center Table Data

# %%
cost_center_txn_data=cost_center_txn_data[((cost_center_txn_data['voucher_date']<= v_todate) & (cost_center_txn_data['voucher_date'] >= v_fromdate))]
cost_center_txn_data=cost_center_txn_data[[ 'mname', 'groupcode', 'groupname', 'costcentercode', 'costcenter','amt' ]]
cost_center_txn_data.rename(columns={'mname':'Account Name',
                     'groupcode':'Group Code', 
                     'groupname':'Group Name', 
                     'costcentercode':'Cost Center Code',
                     'costcenter':'Cost Center Name', 
                     'amt':'Amount'},inplace=True)
cost_center_txn_data=cost_center_txn_data.groupby(['Account Name','Group Code','Group Name','Cost Center Code','Cost Center Name'])['Amount'].sum().reset_index()

# %% [markdown]
# Accounts Table Data Fine Tune

# %%
accounts_txn_data=accounts_txn_data[['vchno', 'vchdt', 'accountname', 'category','nativecurrency', 'subledger', 
                                     'currency', 'exchange_rate','sexchange_rate', 'dramount', 'cramount','alie',  'directoverheadexp']]
accounts_txn_data=accounts_txn_data[(accounts_txn_data['vchdt']>=v_fromdate) & (accounts_txn_data['vchdt']<=v_todate) &((accounts_txn_data['alie']=='i') | (accounts_txn_data['alie']=='e')) ]
accounts_txn_data=accounts_txn_data[[ 'accountname', 'dramount','cramount', 'alie', 'directoverheadexp']]
accounts_txn_data['Amount']=accounts_txn_data['dramount']-accounts_txn_data['cramount']
accounts_txn_data.rename(columns={'accountname':'Account Name','dramount':'Debit Amount','cramount':'Credit Amount', 
                                  'alie':'ALIE', 'directoverheadexp':'Direct Over Head Ex.'},inplace=True)
accounts_txn_data=accounts_txn_data.groupby(['Account Name'])['Amount'].sum().reset_index()

# %% [markdown]
# Checking the Difference in the Accounts Data

# %%
accounts_txn_data['Type']='Accounts Table'
cost_center_txn_data['Type']='Cost Center Table'
cost_center_summary_data=cost_center_txn_data.groupby(['Type','Account Name']).sum().reset_index()
accounts_txn_data=accounts_txn_data[['Type','Account Name', 'Amount']]

accounts_cross_chk_pd=pd.concat([cost_center_summary_data,accounts_txn_data])
accounts_cross_chk_pd=accounts_cross_chk_pd.pivot(index='Account Name',columns=['Type'],values='Amount').reset_index()
accounts_cross_chk_pd['Difference']=accounts_cross_chk_pd['Accounts Table']-accounts_cross_chk_pd['Cost Center Table']
accounts_cross_chk_pd=accounts_cross_chk_pd[accounts_cross_chk_pd['Difference']!=0]

# %% [markdown]
# 

# %% [markdown]
# ## 3. GENERATING FINAL COST SHEET

# %%
logfile.write(f'\nFinal Calculations are started at {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
final_cost_sheet=fg_data

# %% [markdown]
# Join with BOMS

# %%
#Join with Coloring BOM
final_cost_sheet.head(1)

# %% [markdown]
# ## 4. EXCEL WRITING

# %% [markdown]
# ### Export the Masters

# %% [markdown]
# #### Export Item Master

# %%
######### EXCEL EXPORTS #############
logfile.write(f'\nItem Master is writing to excel at {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
with pd.ExcelWriter(folder_name+"/Masters.xlsx") as writer:
     fg_data_exp.to_excel(writer,sheet_name="FG Item Master",index=False)
     item_master_data.to_excel(writer,sheet_name="Item Master",index=False)

# %% [markdown]
# #### Export Rate Master

# %%
logfile.write(f'\nRates are writing to excel at {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
with pd.ExcelWriter(folder_name+"/ItemRateMaster.xlsx") as writer:
     fg_data.to_excel(writer,sheet_name="FG Item Master",index=False)
     item_master_data.to_excel(writer,sheet_name="Item Master",index=False)
     standard_rate_data.to_excel(writer,sheet_name="Standard Rates",index=False)

# %% [markdown]
# #### Export BOM

# %%
logfile.write(f'\nBOM are writing to excel at {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
with pd.ExcelWriter(folder_name+"/BOM_Sheets.xlsx") as writer:
    fg_rm_bom_full_data_exp.to_excel(writer,sheet_name="Furnace Costing",index=False)  #Furnace BOM
    fg_rm_bom_summary_data_exp.to_excel(writer,sheet_name="Furnace Summary",index=False)
    fg_pkg_bom_full_data_exp.to_excel(writer,sheet_name="Packing Costing",index=False)
    fg_pkg_bom_summary_data.to_excel(writer,sheet_name="Packing Summary",index=False)
    screen_full_data_exp.to_excel(writer,sheet_name="Screening Costing",index=False)
    color_full_data_exp.to_excel(writer,sheet_name="Coloring Costing",index=False)
    color_summary_data_exp.to_excel(writer,sheet_name="Coloring Summary",index=False)
    coating_full_data_exp.to_excel(writer,sheet_name="Coating Costing",index=False)
    coating_summary_data_exp.to_excel(writer,sheet_name="Coating Summary",index=False)
    bomdespatch_full_data_exp.to_excel(writer,sheet_name="Despatch Costing",index=False)
    bom_labcost_full_data_exp.to_excel(writer,sheet_name="Labour Costing",index=False)

# %% [markdown]
# #### Export Over Heads

# %%
logfile.write(f'\nOverheads are writing to excel at {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
with pd.ExcelWriter(folder_name+"/Overheads.xlsx") as writer:
    cost_pool_data.to_excel(writer,sheet_name="CostPoolMaster",index=False)
    cost_center_txn_data.to_excel(writer,sheet_name="CostCenterData",index=False)
    acl_running_days_data.to_excel(writer,sheet_name="ACL Running Days",index=False)

# %% [markdown]
# #### Export Final Cost Sheet

# %%
logfile.write(f'\nFinal Cost Sheet is writing to excel at {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
with pd.ExcelWriter(folder_name+"/Final_Cost_Sheet.xlsx") as writer:
    final_cost_sheet.to_excel(writer,sheet_name="Cost Sheet",index=False)  #Furnace BOM

# %% [markdown]
# #### Exception Reports

# %%
logfile.write(f'\nExceptional Reports are writing to excel at {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
with pd.ExcelWriter(folder_name+"/Exceptionals.xlsx") as writer:
    accounts_cross_chk_pd.to_excel(writer,sheet_name="Cost Center Differences",index=False)  #Cost Center Differences

print('Costing Processing is done!!')

# %%
logfile.write(f'\nThe Process is Completed Successfully at {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
logfile.close()


