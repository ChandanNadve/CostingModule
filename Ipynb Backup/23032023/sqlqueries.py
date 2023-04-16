# 1.FG Query
fg_query = ''' select 'Item Master' as "Item Master",fp.productgroup as "Product Group",dc.decorationtype as "Decoration Type", 
cm.glclrname as "Color Name", j.job as "Job", nf.name as "Neck Finish Name",cp.capacity as "Capacity",
pm.process as "Process",lp.layersppallet as "Layers/Pallet", bf.bottleconfiguration,
fp.productid as "FG Code",fp.productdesc as "FG Name",fp.bptray "Qty/Tray",fp.tppallet as "Tray/Pallet",fp.qtyofpallet as "Qty/Pallet",getweight(pmbasicid,trunc(sysdate)) "Weight in Grams" ,fp.finpmbasicid,p.pmbasicid
from finpmbasic fp,decoration dc,colormaster cm,jobmaster j,neckfinsihmaster nf,capacitymaster cp,processmaster pm,layersppallet lp,bottleconfiguration bf,pmbasic p
where fp.cancel ='F'
and fp.decoration = dc.decorationid
and j.jobmasterid = fp.jobno
and lp.layersppalletid = fp.layersppallet
and cm.colormasterid = fp.glasscolor
and nf.neckfinsihmasterid = fp.neckfinsih
and cp.capacitymasterid = fp.capacity
and pm.processmasterid=fp.process
and p.sourceid = fp.finpmbasicid
and bf.bottleconfigurationid = fp.bottleconfig
union all
select 'Prospective Item Master' as "Item Master",fp.productgroup as "Product Group",dc.decorationtype as "Decoration Type", 
cm.glclrname as "Color Name", j.job as "Job", nf.name as "Neck Finish Name",cp.capacity as "Capacity",
pm.process as "Process",lp.layersppallet as "Layers/Pallet", bf.bottleconfiguration,
fp.Itemcode as "FG Code",fp.itemname as "FG Name",fp.bptray "Qty/Tray",fp.tppallet as "Tray/Pallet",fp.qtyofpallet as "Qty/Pallet",botteleweight "Weight in Grams" ,fp.prospitemmasterid,0 as itemmasterid
from prospitemmaster fp,decoration dc,colormaster cm,jobmaster j,neckfinsihmaster nf,capacitymaster cp,processmaster pm,layersppallet lp,bottleconfiguration bf
where fp.cancel ='F'
and fp.decoration = dc.decorationid(+)
and j.jobmasterid(+) = fp.jobno
and lp.layersppalletid(+) = fp.layersppallet
and cm.colormasterid(+) = fp.glasscolour
and nf.neckfinsihmasterid(+) = fp.neckfinishtype
and cp.capacitymasterid(+) = fp.capacity
and pm.processmasterid(+)=fp.process
and bf.bottleconfigurationid(+) = fp.bottleconfig
'''

# 2. RM BOM Header Query
rmbom_header_query = '''select rmbomhdrid,docid,docdt,color,furnance,name,appdate
from rmbomhdr
where cancel='F' '''

# 3. RM BOM Details Query
rmbom_detail_query = '''
select rmbomdtlid,
rmbomhdrid,
rmbomdtlrow,
rmitemname,
rmitemcode,
rmunitco,
qtyperbatch,
evloss,
glassdraw,
rmefficiency,
rmnetpacked,
culletqty,
remarks
from rmbomdtl
'''

# 4. Furnace Master Query
furnace_query='''select psysfurnaceid,fname furnacename,psysfurnacerow as furnaceno from psysfurnace'''

# 5. Color Master Query
color_query='''select colormasterid,glclrname colorname,glclrcode colorcode from colormaster'''

# 6. Standard Efficiency Query
std_eff_query='''select se.docdate,se.docid,sd.color,se.name,sd.furnance,sd.efficiency
from standfureff se,standfureffdtl sd
where se.standfureffid = sd.standfureffid
and se.cancel='F'
 '''

# 7. Costing Process Query
costing_process_query='''select docdate,
       docid,
       pricetype,
       efficiencytype,
       rawmaterialbomtype,
       packingbomtype,
       dispatchtype,
       colorstype,
       standardeference,
       coattype coatingtype,
       screeningtype,
       month,
       year,
       processflag,
       name,
       processcostingid,
       fromdate,
       todate
  from processcosting
 where cancel = 'F'
 and processflag='F'   
 '''

# 8. Item Master Query
item_master_query='''
select p.pmbasicid,p.productgroup,p.productid,p.profitcode,p.productdesc,p.primaryunit ,pc.procategcode,pc.procategory
from pmbasic p,pcategory pc 
where p.cancel='F' 
and pc.pcategoryid=p.itemcategory
'''

# 9. Standard Rate Header Query
standard_rate_header_query= ''' select appdate,name,stdprice_hdrid from stdprice_hdr where cancel='F' '''

# 10. Standard Rate Details Query
standard_rate_details_query= ''' select stdprice_dtlid,stdprice_hdrid,stdprice_dtlrow,itemid,unit,perqty,rate,
conversionfactor,consumptionunit,priceinuom,purcurrency,exrate 
from stdprice_dtl '''

# 11. UOM Query
uom_query = ''' select uomid,unitcode from uom where cancel='F'  '''

# 12. BOM Packing Header Query
pkg_bom_header_query=''' select bm.bompackmaterialhrdid,
       appdate,
       fgitemcode,
       fgitemname,name,
       packingtype,
       traycode,
       bottbox,
       boxlayer,
       boxpallet,
       boxcount,
       bottlespercontainer
  from bompackmaterialhrd bm
 where bm.cancel = 'F'  '''

# 13. BOM Packing Detail Query
pkg_bom_detail_query='''select bompackmaterialhrdid,bompackmaterialdtlid,matcode,matdesc,unitst,consumptionunit,qtyco,qtycon 
from bompackmaterialdtl bd'''

# 14. Reference Name Query
reference_name_query=''' select referencenameid,referencecode,referencename ,inactive
from referencename where cancel='F' '''

# 15. BOM Cost for Labors Header Query
bom_labcost_hdr_query=''' select bom_labcost_hdrid, docid,DOCDT,appdate,name,narration
from bom_labcost_hdr bh
where cancel = 'F'   '''

# 16. BOM Cost for Labors detail Query
bom_labcost_dtl_query=''' select bd.bom_labcost_dtlid ,bom_labcost_hdrid,bd.laborcategory,bd.rate,bd.remarks
from bom_labcost_dtl bd '''

# 17. BOM Despatch Header Query
bomdespatchhdr_query=''' select bomdespatchhdrid,docid,docdate,appdate,name,despatch,despatchcode,despatchname
from bomdespatchhdr bo
where cancel = 'F' 
'''

# 18. BOM Despatch Detail Query
bomdespatchdtl_query=''' select bomdespatchdtlid,bomdespatchhdrid,itemname,itemcode,unitco,qty,remarks
from bomdespatchdtl bd'''

# 19.BOM Coating Header Query
bomcoatinghdr_query=''' select bomcoatinghdrid,docid,docdt,appdate,name,line,linedesc,linespeed
from bomcoatinghdr br
where cancel = 'F' '''

# 20. BOM Coating Detail Query
bomcoatingdtl_query=''' select bomcoatingdtlid,bomcoatinghdrid,coatingitemname,coatingitemcode,coatingunitco,coatqty,coatremarks
from bomcoatingdtl'''

# 21.BOM Colors Header Query
bomcolorshdr_query=''' 
select bomcolorshdrid,docdt,docid,appdate,name,fgitemcode,fgitemname,stdbottqty
from bomcolorshdr
where cancel = 'F' '''

# 22.BOM Colors Detail Query
bomcolorsdtl_query=''' select bomcolorsdtlid,bomcolorshdrid,matcode,matdesc,unitst,consumptionunit,conversionfactor,qtyco,remarks
from bomcolorsdtl '''

# 23. Screening Header Query
screenhdr_query="""select bomcolorshdr_scrid,docdt,appdate,name,fgitemcode
from bomcolorshdr_scr
where cancel ='F' """

# 24. Screening Detail Query
screendtl_query="""select bomcolorshdr_scrid,bomcolorsdtl_scrid,matcode,consumptionunit,conversionfactor,qtyco
from bomcolorsdtl_scr """

# 25. Selected Items for Costing Query
selecteditem_query= ''' select fgitemcode,fgitemname,processcostingdtlid,remarkss 
from processcostingdtl '''

# 26. Line Master Data
linemaster_query="""select psyslinemasterid,lineid,linedesc from psyslinemaster"""

# 27. Accounts Header Query
accounts_txn_query="""
select accountshdrid,
vchno,
vchdt,
accountname,
category,
nativecurrency,
subledger,
currency,
exchange_rate,
sexchange_rate,
ndramount,
ncramount,
dramount,
cramount,
sdramount,
scramount,
interac,
accountsdtlrow,
masterid,
subledgerid,
mapname,
sourceid,
mapname1,
sourceid1,
alie,
directoverheadexp
from vw_accountsdetail
"""


# 28. Cost Center Query
cost_center_txn_query="""
select voucher_number,
voucher_date,
transid,
mapname,
mname,
drorcr,
groupcode,
groupname,
costcentercode,
costcenter,
amt,
dramtbase,
cramtbase,
tstruct,
recid,
accountshdrid    
from vw_costcenter
"""

# 29. Cost Pool Query
cost_pool_query="""
select costpoolid,
       cpcode,
       cpname,
       cptype
  from costpool
"""
  
#30. Tray Master Query
tray_master_query="""
select psystraymasterid,tray_code,tray_name
from psystraymaster
"""

#31. Packing BOM Codes
packing_bom_code_query="""
select c.csysbommasterid,c.bom_code,c.bom_name
from csysbommaster c
order by 2
"""

#31. Currency Master
currency_query="""
select currencyid,currency from currency where cancel='F'
"""

#32. Sales Query
sales_query="""
select docdt,custcode,customercode as customername,productcode,productdesc,
 sum (qty) qty,
         sum (amountinomr) amtinomr,
         sum (weight) mt, sum (frtsplit) freightomr,
         sum (amountinomr) - sum (frtsplit) exworksomr from vw_sales_fin
         group by docdt,custcode,customercode ,productcode,productdesc
"""

#33. Despatch Code Master
despatchcode_query=''' select csystdespatchid,
       desp_code,
       desp_name,
       desp_details
  from csystdespatch
  '''
  
#34. Labour Code Master
labourcode_query='''
  select csyslaborcategoryid,categorycode,laborcategory
  from csyslaborcategory
  '''       
     
#35. Over Head Basis
overhead_basis_query='''
select a.allocationbasic,a.basis_qty,a.remarks
from overheadbasis a
'''

#36. Actual Over Heads
overhead_distribution_query='''
select a.overhead_acct,a.ohid,a.oh_code,a.overhead_exp,a.overhead_nm
from act_oh_cost_t a
'''

#37. Efficiency Query
efficiency_query='''select pb.docdt,
       lm.lineid,lm.linedesc,
       substr (lm.lineid, 1, 1) furnance,
       im.productid,
       im.productdesc,
       pd.bspeed,
       pd.speed,
       pd.wt,
       pd.bprbottles,
       pd.prbottles,
       pd.packbottles,
       pd.bprodtonnage,
       pd.prodtonnage,
       pd.packtonnage,
       pd.eff,
       pd.inscapacity,
       im.pmbasicid
  from peffbasic pb,
       peffdetail pd,
       psyslinemaster lm,
       pmbasic im,psysshift s,psysmachine m
 where     pb.cancel = 'F'
       and pb.peffbasicid = pd.peffbasicid
       and im.pmbasicid = pd.itemcode
and lm.psyslinemasterid = pd.lineid
        and m.psysmachineid=lm.mname
        and s.psysshiftid=pd.shift
and lm.lineid not in ('ACL-1','ACL-2','OFF LINE-1')
'''

#38. Material Issue Report
issue_query='''
select sth.docdt as docdt,std.productid,
         std.qty as issueqty,rate,amount
         from stockissuehdr sth,
         stockissuedtl std
   where     sth.cancel ='F'
         and sth.stockissuehdrid = std.stockissuehdrid
 union all
 select sth.docdt as docdt,std.productid,
         -std.qty as issueqty,-std.rate,-std.amount
from projectreceiptbasic sth,projectreceiptdetail std
where sth.projectreceiptbasicid=std.projectreceiptbasicid
and sth.cancel='F'
'''

#39. FG Stock Query
fg_stock_query='''
select p.pmbasicid,docdate,sum(received-issued) as qty
from v_stockvalue v,pmbasic p
where v.itemid=p.pmbasicid
and p.productgroup in ('FINISHED GOODS','WORK IN PROGRESS')
group by p.pmbasicid,docdate
'''

#40. Opening Costing for 2021 Query
opening_costing_query='''
select item_code1,item_code1_f,
item_desc1,process_code,weight_gms,
qty_pallet,line_code,lcode,line_desc,line_speed,efficiency,
g_code,gob_code,gob_desc,color,furnace,option_code,
item_code1_f2,fur_clor,rm_costkg,lin_oh,uti_oh,item_code2,item_desc2,weight_gms1,
qty_pallet1,print_speed,item_code2_f,efficiency1,item_code3,item_desc3,weight_gms2,qty_pallet2,
stage_code,stage_description,despatch_code,despatch_description,
pack_code,pack_type,lin_oc,lin_sec,lin_oh1,lin_oh2,
lin_oh3,cot_day,acot_day,p_code,s_code,colco,
l_code,descode,s_codi,l_codi,pkg_cost,rmt_cost,mld_cost,
cot_cost,acl_cost,acot_cost,pks_cost,pki_cost,los_cost,s_lco,s_lci,oh_min,
ut_min,acl_oh,gen_oh,fin_oh,desp_cost,copn_bott_ro,
cogs_bott_ro,tcos_bott_ro,copn_mt_ro,cogs_mt_ro,tcos_mt_ro,copn_mt_usd,
cogs_mt_usd,tcos_mt_usd,copn_bott_usd,cogs_bott_usd,tcos_bott_usd,sku_code,
sku_type,cas_oh,dep_cost,cash_bott_ro,cash_bott_usd,
variable_cost,fixed_mfg_cost,admin_cost,finance_cost,total_cost    
from costingopening
'''

#41. ACL Running Days
acl_running_days_query='''
select  lineid as line,count(docdate) noofdays1,last_day(docdate) as lastday
from (select M.LINEID, DB.DOCDATE
from dirprodbasic db,dirprodopdetails dd,psyslinemaster m
where db.dirprodbasicid = dd.dirprodbasicid
and db.cancel='F'
and m.psyslinemasterid = DD.LINEID
and M.LINEID like 'ACL%'
group by M.LINEID, DB.DOCDATE)
group by lineid,to_char(docdate,'YYYY-MM(MON)') ,last_day(docdate)
order by 3,1
'''

#42. Direct Over Head Expenses
direct_overhead_exp_query='''
select m2.mname parentacc,a.mname,groupcode,groupname,costcentercode,costcenter,sum(dramtbase-cramtbase) creditamount
from vw_costcenter a,master m,master m2
where voucher_date between :fromdate and :todate
and (upper(trim(a.mname)) = upper(trim( :accountname )) or 'ALL' =upper(trim( :accountname)))
and m.mname= a.mname
and m.mparent = m2.masterid
and m.directoverheadexp='T'
group by m2.mname,costcentercode,a.mname,costcenter,groupcode,groupname
'''

#43. FG Items Configuration for Costing
fg_item_config_query='''
SELECT a1.docid,
       a1.docdt,
       a1.costingname,
       a2.customername AS customername,
       a3.productdesc AS FinalItemName,
       a1.fitemcode as FinalItemCode,
       a4.productdesc AS PlainItemName,
       a1.itemcode as PlainItemCode,
       a5.productdesc AS PrintedItemName,
       a1.itemcode1 as PrintedItemName,
       a6.productdesc AS BoxFloorItemName,
       a1.itemcode2 as BoxFloorItemCode,
       a7.productdesc AS BoxPalletItemName,
       a1.itemcode3 as BoxPalletItemCode,
       a1.inactive,
       a1.inactivedate,
       a1.inactivereason,
       a8.bptray,
       a8.trayperpallet,
       a8.qtyofpallet,
       a8.bottbox,
       a8.boxlayer,
       a8.boxpallet,
       a8.boxcont,
       a8.palletcont,
       a8.bottcont,
       a8.weight,
       a9.stagecode AS stagecode,
       a8.stagedesc,
       a10.desp_code AS despatchcode,
       a8.despatchname,
       a12.linedesc AS linename,
       a11.lineid,
       a11.efficiency,
       a11.speed,
       a11.gob,
       a11.section,
       a11.cavity,
       a11.lremarks
  FROM fgbom a1,
       vw_customer a2,
       vw_item2 a3,
       vw_item2 a4,
       vw_item2 a5,
       vw_item2 a6,
       vw_item2 a7,
       fgdetails a8,
       csysstage a9,
       psystdespatch a10,
       fgbomline a11,
       psyslinemaster a12
 WHERE     a1.customername = a2.vw_customerid(+)
       AND a1.fitemname = a3.vw_item2id(+)
       AND a1.itemname = a4.vw_item2id(+)
       AND a1.itemname1 = a5.vw_item2id(+)
       AND a1.itemname2 = a6.vw_item2id(+)
       AND a1.itemname3 = a7.vw_item2id(+)
       AND a1.fgbomid = a8.fgbomid(+)
       AND a8.stagecode = a9.csysstageid(+)
       AND a8.despatchcode = a10.psystdespatchid(+)
       AND a1.fgbomid = a11.fgbomid(+)
       AND a11.linename = a12.psyslinemasterid(+)
  and a1.cancel='F'
'''

#44. Gas and Power Rate Master
gas_power_query='''SELECT   a1.asondate,
       a1.TYPE,
       a1.utype,
       a1.rate,
       a3.lineid AS line,
       a2.unitsphour
  FROM gaspowersetting a1, gaspowerline a2, psyslinemaster a3
 WHERE     a1.gaspowersettingid = a2.gaspowersettingid(+)
       AND a2.line = a3.psyslinemasterid(+)'''
       
#45. IS Machine Downtime Query       
ism_downtime_query= '''select s.docdate,s.linedesc, s.machinename,' Machine' Machine,s.docid,r.reason,m.Stoppeddate Stopdate,m.stoppedtime stoptime,
m.restartdt restartdate,m.restarttm restarttime,sa.timemin,m.remark as remark from machinestop m,  reason r, 
sectionrestart s,(  select machinestopid, (days*24*60)+(hours*60)+minutes as timemin from(
select machinestopid,
   extract( day from diff ) Days, 
   extract( hour from diff ) Hours,
   extract( minute from diff ) Minutes 
from (
       select machinestopid, (CAST(ft as timestamp ) - CAST( lt as timestamp)) diff   
       from ( select machinestopid,TO_DATE(TO_CHAR(restartdt, 'DD-MON-YYYY') || ' ' ||TO_CHAR(TO_DATE(restarttm,'HH24:MI:SS'),'HH24:MI:SS'), 
       'DD-MON-YYYY HH24:MI:SS') ft, TO_DATE(TO_CHAR(Stoppeddate, 'DD-MON-YYYY') || ' ' ||TO_CHAR(TO_DATE(stoppedtime,'HH24:MI:SS'),'HH24:MI:SS'), 
       'DD-MON-YYYY HH24:MI:SS') lt from machinestop where restartdt is not null)))) sa
where m.reasonm = r.reasonid
and m.sectionrestartid = s.sectionrestartid
and sa.machinestopid = m.machinestopid
union all
select s.docdate,s.linedesc, s.machinename, ls.sectionname Machine,s.docid,r.reason,m.Stoppagedate Stopdate,m.stoppagetime stoptime, 
m.restartdate restartdate,m.restarttime restarttime ,s3.timemin,m.remarks as remark
from sectionstop m,  reason r, 
linesection ls,sectionrestart s,  ( select sectionstopid, (days*24*60)+(hours*60)+minutes as timemin from(
select sectionstopid,
   extract( day from diff ) Days, 
   extract( hour from diff ) Hours,
   extract( minute from diff ) Minutes 
from (
       select sectionstopid, (CAST(ft as timestamp ) - CAST( lt as timestamp)) diff   
       from ( select sectionstopid,TO_DATE(TO_CHAR(restartdate, 'DD-MON-YYYY') || ' ' ||TO_CHAR(TO_DATE(restarttime,'HH24:MI:SS'),'HH24:MI:SS'), 
       'DD-MON-YYYY HH24:MI:SS') ft, TO_DATE(TO_CHAR(stoppagedate, 'DD-MON-YYYY') || ' ' ||TO_CHAR(TO_DATE(stoppagetime,'HH24:MI:SS'),'HH24:MI:SS'), 
       'DD-MON-YYYY HH24:MI:SS') lt from sectionstop)))) s3
where m.reasons = r.reasonid
and m.section = ls.LINESECTIONID
and m.sectionrestartid = s.sectionrestartid
and s3.sectionstopid = m.sectionstopid'''

