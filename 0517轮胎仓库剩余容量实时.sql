USE [TuHuBI_DM]
GO
/****** Object:  StoredProcedure [dbo].[sp_dm_RemainStockCapacity_Realtime]    Script Date: 2019/5/16 14:02:55 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER procedure [dbo].[sp_dm_RemainStockCapacity_Realtime]
as
/*
createuser:徐玢花
datetime：2017/8/3
descripition:仓库剩余库存容量（每隔1h更新一次）
修改：xubinhua 20180108 济南仓的 手工调整下 理论算北京、理论的 也不算工场店发货、未知的 理论也按实际的算
修改：xubinhua 20180118  将下单时间改成订单完成时间，考虑红冲订单数据，订单数据取全部的途虎订单数据
修改：增加一个新的字段TireSkuQuantity，记录仓库实时的有库存的轮胎Sku数， 张锋(2018/5/18)
修改：增加两个个新的字段[78周内库存]和[超78周库存] 张锋(2018/5/21)
修改：liufan 20180910 去掉重庆二号仓库限制
*/

/*
需求人：肖志飞
修改人：陈莉20190326
修改：----不良品库存（不可用库存）拆分成两部分   陈莉
           --不良品库存（缺标签库存）逻辑可参照缺标签周报
           --不良品库存（不可用库存）剔除缺标签剩余的不良品库存*/
--修改：
     --增加若干汽配龙仓库 20190517
 IF OBJECT_ID('tempdb.dbo.#wr') IS NOT NULL
    DROP TABLE #wr;
select WareHouseid,Warehouse,warehousetype,isactive
into #wr
from bi44.TuHuBI_DW.dbo.dw_WareHouse wr WITH ( NOLOCK )
where wr.warehousetype='tire'
or WareHouse in (
'汽配龙南京栖霞仓库',
'汽配龙虹梅南路仓库',
'汽配龙上海浦东金桥仓库',
'汽配龙武汉蓝焰物流基地仓库')
--采购在途
 IF OBJECT_ID('tempdb.dbo.#TempPurchaseOrder') IS NOT NULL
    DROP TABLE #TempPurchaseOrder;
 SELECT poi.PKID ,
         poi.WareHouseID ,
         poi.PID ,
        poi.Num ,
         poi.InstockNum ,
         poi.Status ,
         poi.PurchaseMode
 INTO   #TempPurchaseOrder
 FROM   Gungnir.dbo.PurchaseOrderItem poi WITH ( NOLOCK )
  INNER JOIN bi44.TuHuBI_DW.dbo.dw_PurchaseMode AS PM WITH ( NOLOCK ) -- 采购方式表
              ON PM.PurchaseMode = POI.PurchaseMode
                    AND PM.Category = '正常'--若需取正常采购单或原本限制purchasemode in ( 0, 1, 2, 3, 4, 7, 8, 9, 10 ) ,加该条件
inner join #wr wr   WITH ( NOLOCK )
  on poi.WareHouseid=wr.WareHouseid 
 WHERE  ( Status = N'新建'
          OR Status = N'已发货'
          OR Status = N'部分收货'
        )
        AND Num > 0
        AND PID LIKE 'tr-%'
        AND PID != 'BX-TUHU-LTX|'
		--AND ISNULL(poi.WareHouse,'未知') <> '重庆二号仓库'
        --AND PurchaseMode IN ( 0, 1, 2, 3, 4, 7, 8, 9, 10 );



 IF OBJECT_ID('tempdb.dbo.#caigouzaitu') IS NOT NULL  --select * from #caigouzaitu
    DROP TABLE #caigouzaitu;
 SELECT wh.WareHouse ,
        dfs.WareHouseID ,
       -- dfs.PID ,
        SUM(ISNULL(tb1.Num, 0) + ISNULL(tb2.Num, 0)) AS num,--采购在途
        SUM(ISNULL(tb3.BookingQuantity, 0)) AS BookingQuantity --预约在途
 INTO   #caigouzaitu
 FROM   ( SELECT DISTINCT
                    dfs.WareHouseID ,
                    dfs.PID
          FROM      #TempPurchaseOrder AS dfs
        ) AS dfs
        INNER JOIN BI44.TuHuBI_DW.dbo.dw_WareHouse AS wh WITH ( NOLOCK ) ON wh.WareHouseid = dfs.WareHouseID
        LEFT  JOIN ( SELECT WareHouseID ,
                            PID ,
                            SUM(Num - ISNULL(InstockNum, 0)) AS Num
                     FROM   #TempPurchaseOrder
                     WHERE  PurchaseMode <> 8
                     GROUP BY WareHouseID ,
                            PID
                   ) AS tb1 ON tb1.WareHouseid = dfs.WareHouseID
                               AND tb1.PID = dfs.PID --COLLATE Chinese_PRC_CI_AS
        LEFT JOIN ( SELECT  WareHouseID ,
                            PID ,
                            SUM(ISNULL(PR.RelatedNum, 0)) Num
                    FROM    #TempPurchaseOrder AS TPO
                            LEFT JOIN Gungnir.dbo.PurchaseReverse
                            AS PR WITH ( NOLOCK ) ON PR.PoId = TPO.PKID
                                                     AND PR.IsDeleted = 0
                                                     AND PR.IsAudited = 1
                    WHERE   PurchaseMode <> 8
                    GROUP BY WareHouseID ,
                            PID
                  ) AS tb2 ON tb2.WareHouseid = dfs.WareHouseID
                              AND tb2.PID = dfs.PID --COLLATE Chinese_PRC_CI_AS
        LEFT JOIN ( SELECT  WareHouseID ,
                            PID ,
                            SUM(Num - ISNULL(InstockNum, 0)) AS BookingQuantity
                    FROM    #TempPurchaseOrder
                    WHERE   PurchaseMode = 8
                    GROUP BY WareHouseID ,
                            PID
                  ) AS tb3 ON tb3.WareHouseid = dfs.WareHouseID
                              AND tb3.PID = dfs.PID
   GROUP BY wh.WareHouse, dfs.WareHouseID --, dfs.PID 

							  


--移库在途
IF OBJECT_ID('tempdb..#yikuzaitu') IS NOT NULL 
DROP TABLE #yikuzaitu
 SELECT  LTA.TargetName  , lta.targetid,
                            --LTP.PID ,
                            SUM(LTS.Num) AS num ,
                            SUM(LTS.Num * B.CostPrice) AS TRANSFERPRICE into #yikuzaitu
                    FROM    [WMS].dbo.LogisticTask AS LT WITH ( NOLOCK )
                            INNER JOIN [WMS].dbo.LogisticTaskProduct
                            AS LTP WITH ( NOLOCK ) ON LT.PKID = LTP.LogisticTaskId
                            INNER JOIN [WMS].dbo.LogisticTaskAddress
                            AS LTA WITH ( NOLOCK ) ON LTA.LogisticTaskID = LT.PKID
                            INNER JOIN [WMS].dbo.LogisticTaskStock
                            AS LTS WITH ( NOLOCK ) ON LTS.LogisticTaskProId = LTP.PKID
                            INNER JOIN [WMS].dbo.StockLocation AS SL
                            WITH ( NOLOCK ) ON SL.PKID = LTS.StockId
                            INNER JOIN [WMS].[dbo].[Batch] AS B WITH ( NOLOCK ) ON B.PKID = SL.BatchId

                            inner join #wr wr   WITH ( NOLOCK )
                            on lta.targetid=wr.WareHouseid 
                    WHERE   LT.TaskStatus = '3Sent'--发出
                            AND LT.OrderType = '1Remove'--移库
                            AND LTP.Num - LTP.ReceivedNum > 0

							and LTP.PID like 'tr-%'
                    GROUP BY --LTP.PID ,
                            LTA.TargetName , lta.targetid


--库存信息
IF OBJECT_ID('tempdb..#ProductStockLocation') IS NOT NULL 
DROP TABLE #ProductStockLocation
SELECT  
	b.PKID AS BatchId,      /****获取库存信息****/
    SL.LocationId,
    SL.PID,
	b.COSTPRICE,
    SUM(SL.Num) AS StockQuantity,
    b.CostPrice * SUM(SL.Num) AS TotalCost,
	CASE 
		WHEN Len(B.weekyear)=4 
			 AND patindex('%[^0-9]%',B.weekyear)=0
			 AND SL.pid like 'tr-%'
			 AND (RIGHT(B.weekyear,2)<RIGHT(datepart(year,dateadd(week,-52,getdate()-1)),2) 
				  OR 
				 (RIGHT(B.weekyear,2)=RIGHT(datepart(year,dateadd(week,-52,getdate()-1)),2)
				  AND LEFT(B.weekyear,2)<DATEPART(WEEK,DATEADD(week,-52,getdate()-1))))
			THEN 0
		ELSE 1
	END AS weekyearmark,
	CASE 
		WHEN Len(B.weekyear)=4 
			 AND patindex('%[^0-9]%',B.weekyear)=0
			 AND SL.pid like 'tr-%'
			 AND (RIGHT(B.weekyear,2)<RIGHT(datepart(year,dateadd(week,-78,getdate()-1)),2) 
				  OR 
				 (RIGHT(B.weekyear,2)=RIGHT(datepart(year,dateadd(week,-78,getdate()-1)),2)
				  AND LEFT(B.weekyear,2)<DATEPART(WEEK,DATEADD(week,-78,getdate()-1))))
			THEN 0
		ELSE 1
	END AS weekyearmark1,
	SL.StockType
INTO  #ProductStockLocation
FROM    [WMS].dbo.StockLocation AS SL WITH (NOLOCK)
INNER JOIN [WMS].dbo.Batch AS b WITH (NOLOCK)
		ON SL.BatchId = b.PKID
      inner join #wr wr   WITH (NOLOCK)
  on sl.locationid=wr.WareHouseid 
WHERE   BatchId != 0
		AND SL.Num>0
		and SL.pid like 'tr-%'--只取轮胎
		--AND ISNULL(sl.Location,'未知') <>'重庆二号仓库'

GROUP BY b.PKID,
		SL.LocationId,
		SL.PID,
		b.CostPrice,
		b.WEEKYEAR,sl.StockType



--订单占用库存
IF OBJECT_ID('tempdb..#OrderOccupation') IS NOT NULL 
DROP TABLE #OrderOccupation
SELECT  psl.LocationId,isnull(SUM(Num),0) AS NUM into #OrderOccupation
                        FROM    [Gungnir].dbo.SoStock AS SS
                                WITH ( NOLOCK )
                                INNER JOIN [Gungnir].dbo.tbl_Order
                                AS O WITH ( NOLOCK ) ON SS.SoId = O.PKID
								inner join #ProductStockLocation  psl
								on ( (O.WareHouseID = psl.LocationId
                                    AND O.DeliveryDatetime IS NULL
                                  )
                                  OR SS.SoId = 1
                                  AND SS.LocationId = psl.LocationId
                                )
                                AND SS.BatchId = psl.BatchId  
                            group by psl.LocationId


--待移库库存
IF OBJECT_ID('tempdb..#TransferOccupation') IS NOT NULL 
DROP TABLE #TransferOccupation
SELECT  psl.LocationId,ISNULL(SUM(Num), 0) AS NUM  into #TransferOccupation
                        FROM    [Gungnir].dbo.TransferStock
                                AS TS WITH ( NOLOCK )
                                INNER JOIN [Gungnir].dbo.WarehouseTransfer
                                AS WT WITH ( NOLOCK ) ON TS.TransferId = WT.PKID
								inner join #ProductStockLocation  psl
                                on  WT.SourceWarehouse = psl.LocationId
                                AND TS.BatchId = psl.BatchId 
                                where WT.TransferStatus IN ( 0, 1 )           
                        group by psl.LocationId


------不良品
----IF OBJECT_ID('tempdb..#Defective') IS NOT NULL 
----DROP TABLE #Defective
----select psl.locationid,sum(psl.StockQuantity) as num into  #Defective
----from #ProductStockLocation psl
---- where psl.StockType=1  
---- group by psl.locationid


--------不良品库存（不可用库存）拆分成两部分   陈莉
          --不良品库存（缺标签库存）逻辑可参照缺标签周报
          --不良品库存（不可用库存）剔除缺标签剩余的不良品库存

IF OBJECT_ID('tempdb..#Defective') IS NOT NULL
DROP TABLE #Defective;
SELECT
  WS.LocationId,
  SUM(sb.num) AS num,  ---不良品合计
  SUM(CASE WHEN  WS.StorageName LIKE 'QBQ%' THEN SB.num ELSE 0 END) AS tirelostNum,  --缺标签
  (SUM(sb.num)-SUM(CASE WHEN  WS.StorageName LIKE 'QBQ%' THEN SB.num ELSE 0 END)) AS untirelostNum   --剔除缺标签剩余数据
INTO #Defective
FROM WMS.dbo.StorageBatch SB with(nolock)
  INNER JOIN WMS.dbo.WarehouseStorage WS with(nolock)
    ON WS.PKID = SB.StorageId
 INNER JOIN  [WMS].dbo.Batch AS b WITH (NOLOCK)
 ON sb.BatchId = b.PKID
       inner join #wr wr   WITH (NOLOCK)
  on ws.locationid=wr.WareHouseid 
WHERE
  SB.StockType = 1 --不良品库存;标缺只是缺标签，产品并未损坏，不算真正的不良品
  AND SB.Num > 0
  AND WS.IsActive = 1
  AND b.PID LIKE 'tr-%'  
  AND SB.BatchId<>0
GROUP BY WS.LocationId

--SELECT * FROM #Defective

---52周/超52周
IF OBJECT_ID('tempdb..#weekyearmark') IS NOT NULL 
DROP TABLE #weekyearmark
SELECT    psl.LocationId ,
                    psl.PID ,
                    psl.StockQuantity ,
                    psl.TotalCost ,
                    psl.CostPrice AS Costprice ,
                    psl.StockQuantity
                    - ( SELECT  ISNULL(SUM(Num), 0) AS NUM
                        FROM    [Gungnir].dbo.SoStock AS SS
                                WITH ( NOLOCK )
                                INNER JOIN [Gungnir].dbo.tbl_Order
                                AS O WITH ( NOLOCK ) ON SS.SoId = O.PKID
                        WHERE   ( ( O.WareHouseID = psl.LocationId
                                    AND O.DeliveryDatetime IS NULL
                                  )
                                  OR SS.SoId = 1
                                  AND SS.LocationId = psl.LocationId
                                )
                                AND SS.BatchId = psl.BatchId
                      )
                    - ( SELECT  ISNULL(SUM(Num), 0) AS NUM
                        FROM    [Gungnir].dbo.TransferStock
                                AS TS WITH ( NOLOCK )
                                INNER JOIN [Gungnir].dbo.WarehouseTransfer
                                AS WT WITH ( NOLOCK ) ON TS.TransferId = WT.PKID
                        WHERE   WT.SourceWarehouse = psl.LocationId
                                AND TS.BatchId = psl.BatchId
                                AND WT.TransferStatus IN ( 0, 1 )
                      ) AS AvailableStockQuantity ,
                    weekyearmark,
					psl.weekyearmark1
					into #weekyearmark
          FROM      #ProductStockLocation AS psl
		  where  psl.StockType=0


IF OBJECT_ID('tempdb..#weekyearmark0') IS NOT NULL 
DROP TABLE #weekyearmark0
select LocationId,sum(AvailableStockQuantity) as num into #weekyearmark0
from  #weekyearmark 
where weekyearmark=0
group by LocationId

IF OBJECT_ID('tempdb..#weekyearmark1') IS NOT NULL 
DROP TABLE #weekyearmark1
select LocationId,sum(AvailableStockQuantity) as num into #weekyearmark1
from  #weekyearmark 
where weekyearmark=1
group by LocationId

IF OBJECT_ID('tempdb..#weekyearmark2') IS NOT NULL 
DROP TABLE #weekyearmark2
select LocationId,sum(AvailableStockQuantity) as num into #weekyearmark2
from  #weekyearmark 
where weekyearmark1=0
group by LocationId

IF OBJECT_ID('tempdb..#weekyearmark3') IS NOT NULL 
DROP TABLE #weekyearmark3
select LocationId,sum(AvailableStockQuantity) as num into #weekyearmark3
from  #weekyearmark 
where weekyearmark1=1
group by LocationId

--计算月销(配置的原仓)--select * from #sales30
IF OBJECT_ID('tempdb..#sales30') IS NOT NULL 
drop table #sales30
select CASE WHEN b.OriginalWarehouse LIKE '%未知%' THEN b.RealWareHouse
ELSE b.OriginalWarehouse END OriginalWarehouse,sum(b.num) as monthnum
into #sales30
from BI44.[TuHuBI_DW].[dbo].[dw_OrderCube]  as b with (nolock) 
inner join #wr  wr
on  (CASE WHEN b.OriginalWarehouse LIKE '%未知%' THEN b.RealWareHouse
ELSE b.OriginalWarehouse end )=wr.Warehouse
where  convert(varchar(10),b.OrderFinishTime,121)<convert(varchar(10),getdate(),121)
and convert(varchar(10),b.OrderFinishTime,121)>=convert(varchar(10),getdate()-30,121)
and pid like 'tr-%' 
--AND num>0 20180118
AND OrderChannel not in (select OrderChannelID from bi44.tuhubi_dw.dbo.dw_OrderChannel where IsFinance=0)--去除特殊渠道
--AND orderchannel NOT LIKE '%v门店货权%'  20180118 

group by CASE 
WHEN OriginalWarehouse LIKE '%未知%' THEN b.RealWareHouse
ELSE OriginalWarehouse END
--SELECT * FROM #sales30 WHERE OriginalWarehouse='济南仓库'

--计算月销(实际发货仓)
IF OBJECT_ID('tempdb..#sales') IS NOT NULL 
drop table #sales
select 
CASE WHEN    b.RealWareHouse IN ('北京仓库','北京北仓库')  THEN '北京仓库' ELSE b.RealWareHouse
END RealWareHouse,sum(b.num) as monthnum
into #sales
from BI44.[TuHuBI_DW].[dbo].[dw_OrderCube]  as b with (nolock)
inner join #wr  wr
on  b.RealWareHouse=wr.Warehouse 
where  convert(varchar(10),b.OrderFinishTime,121)<convert(varchar(10),getdate(),121)

and convert(varchar(10),b.OrderFinishTime,121)>=convert(varchar(10),getdate()-30,121)
and pid like 'tr-%' 
--AND num>0
AND OrderChannel not in (select OrderChannelID from bi44.tuhubi_dw.dbo.dw_OrderChannel where IsFinance=0)--AND orderchannel<>'U门店'
--AND orderchannel NOT LIKE '%v门店货权%'

GROUP by  CASE WHEN    b.RealWareHouse IN ('北京仓库','北京北仓库')  THEN '北京仓库' ELSE b.RealWareHouse
END

--计算周销(实际发货仓)
IF OBJECT_ID('tempdb..#weeklysales') IS NOT NULL 
drop table #weeklysales
select 
CASE WHEN    b.RealWareHouse IN ('北京仓库','北京北仓库')  THEN '北京仓库' ELSE b.RealWareHouse
END RealWareHouse,sum(b.num) as weeklynum
into #weeklysales
from BI44.[TuHuBI_DW].[dbo].[dw_OrderCube]  as b with (nolock) 
inner join #wr  wr
on  b.RealWareHouse=wr.Warehouse
where  convert(varchar(10),b.OrderFinishTime,121)<convert(varchar(10),getdate(),121)
and convert(varchar(10),b.OrderFinishTime,121)>=convert(varchar(10),getdate()-7,121)
and pid like 'tr-%' 
--AND num>0
AND OrderChannel not in (select OrderChannelID from bi44.tuhubi_dw.dbo.dw_OrderChannel where IsFinance=0)--AND orderchannel<>'U门店'
--AND orderchannel NOT LIKE '%v门店货权%'
AND b.RealWareHouse LIKE '%仓库'
GROUP by  CASE WHEN    b.RealWareHouse IN ('北京仓库','北京北仓库')  THEN '北京仓库' ELSE b.RealWareHouse
END








--实际库存数量
IF OBJECT_ID('tempdb..#realStock') IS NOT NULL 
drop table #realStock
SELECT locationid,SUM(StockQuantity) AS StockQuantity  INTO #realStock
FROM #ProductStockLocation
GROUP BY LocationId

--仓库当前的轮胎SKU数量
IF OBJECT_ID('tempdb..#TireSkuQuantity') IS NOT NULL	
	DROP TABLE #TireSkuQuantity

SELECT LocationId, COUNT(DISTINCT PID) AS TireSkuQuantity
INTO #TireSkuQuantity
FROM #ProductStockLocation
WHERE StockQuantity > 0
GROUP BY LocationId


--更新目标表
DELETE FROM [BI232].[TuHuBI_DM].[dbo].[dm_RemainStockCapacity_Realtime]; 
INSERT  INTO [BI232].[TuHuBI_DM].[dbo].[dm_RemainStockCapacity_Realtime]
        ( [仓库] ,
          [仓库最大库容量] ,
          [MONTHSALESQUANTITY] ,
          [realmonthnum] ,
          [StockQuantity] ,
          [52周内库存] ,
          [超52周库存] ,
          [OrderOccupation] ,
          [TransferOccupation] ,
          [Defective] ,
          [zaitu] ,
          [RemainStockCapacity] ,
          [ratio1] ,
          [ratio2] ,
          [orgstockday] ,
          [realstockday] ,
          [ReportDate] ,
          [LoadDate] ,
          [LargestReceipt] ,
          [TireSkuQuantity] ,
          [78周内库存] ,
          [超78周库存] ,
          [realweeklynum],
		  [tirelostnum],
		  [untirelostnum]
        )
SELECT
  b.Location,
  a.StorageDangerCapacity,
  isnull(dfs.monthnum,0) AS MONTHSALESQUANTITY,
  isnull(sl.monthnum,0) AS realmonthnum,
  isnull(rs.StockQuantity,0) as StockQuantity,
  ISNULL(w1.num, 0) AS '52周内库存',
  ISNULL(w0.num, 0) AS '超52周库存',
  ISNULL(oo.NUM, 0) AS OrderOccupation,
  ISNULL(t.NUM, 0) AS TransferOccupation,
  ISNULL(df.num, 0) AS Defective,
  ISNULL(cg.num, 0) + ISNULL(yk.num, 0) AS zaitu,
  isnull(a.StorageDangerCapacity,0) - isnull(rs.StockQuantity,0) AS RemainStockCapacity,
  sl.monthnum * 1.0 / a.StorageDangerCapacity AS ratio1,
  rs.StockQuantity * 1.0 / a.StorageDangerCapacity AS ratio2,
  rs.StockQuantity * 1.0 / (dfs.monthnum * 1.0 / 30) AS orgstockday,
  rs.StockQuantity * 1.0 / (sl.monthnum * 1.0 / 30) AS realstockday,
  CONVERT(VARCHAR(10), GETDATE(), 121) AS reportdate,
  GETDATE() AS loaddate,
  a.LargestReceipt,
  tsq.TireSkuQuantity, --新增字段
  w3.num AS '78周内库存',
  w2.num AS '超78周库存',
  isnull(wk.weeklynum,0) as weeklynum,
  ISNULL(df.tirelostNum, 0) AS tirelostNum,   --缺标签
  ISNULL(df.untirelostNum, 0) AS Defective2   --- --剔除缺标签剩余数据
FROM
  [WMS].dbo.WarehouseUnloadingConfiguration a WITH (NOLOCK) --替换掉原来的固定报表[BI232].[TuHuBI_DM].[dbo].[dm_RemainStockCapacity]
    inner join  #wr  wr
  on a.LocationId=wr.WareHouseid  
  LEFT JOIN [WMS].dbo.Warehouse b WITH (NOLOCK)
    ON a.LocationId = b.LocationId
  LEFT JOIN #sales30 dfs
    ON dfs.OriginalWarehouse = b.Location
  LEFT JOIN #sales sl
    ON sl.RealWareHouse = b.Location
  LEFT join #weeklysales wk
    on wk.RealWareHouse = b.Location
  LEFT JOIN #realStock rs
    ON rs.LocationId = b.LocationId
  LEFT JOIN #weekyearmark0 w0
    ON b.LocationId = w0.LocationId
  LEFT JOIN #weekyearmark1 w1
    ON b.LocationId = w1.LocationId
  LEFT JOIN #OrderOccupation oo
    ON b.LocationId = oo.LocationId
  LEFT JOIN #TransferOccupation AS t
    ON b.LocationId = t.LocationId
  LEFT JOIN #Defective df
    ON b.LocationId = df.LocationId
  LEFT JOIN #caigouzaitu cg
    ON b.LocationId = cg.WareHouseID
  LEFT JOIN #yikuzaitu yk
    ON b.LocationId = yk.targetid
  LEFT JOIN #TireSkuQuantity tsq
	ON a.LocationId = tsq.LocationId
  LEFT JOIN #weekyearmark2 w2
    ON b.LocationId = w2.LocationId
  LEFT JOIN #weekyearmark3 w3
    ON b.LocationId = w3.LocationId

WHERE
 a.StorageDangerCapacity > 1   --将仓库更新为轮胎仓
  --AND b.Location <> '重庆二号仓库';

