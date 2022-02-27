
select bill_acc.*,
	   metnode.met_Id,
	   metnode.met_ServiceId,
	   metnode.met_Supplier,
	   metnode.met_isDeleted,
	   metnode.met_EndDate,
	   bill_prem.*
into #bill_accmetprem
from (
	select Id							    as bill_acc_Id, 
	       IbzkhId						    as bill_acc_IbzkhId, 
		   IbzkhPremiseInformationId        as bill_acc_IbzkhPremId, 
		   Number						    as bill_acc_Number,
		   IsDeleted					    as bill_acc_isDeleted, 
		   EndDate                          as bill_acc_EndDate
    from BillingFund.dbo.PersonalAccounts
	) as bill_acc
	right join (
	select Id							    as met_Id, 
		   PersonalAccountId, 
		   PremiseInformationId, 
		   UtilityServiceId					as met_ServiceId, 
		   SupplierId					    as met_Supplier, 
		   IsDeleted					    as met_isDeleted, 
		   EndDate						    as met_EndDate
	from BillingFund.dbo.MeteringNodes   
	where (PersonalAccountId is not null) and (PremiseInformationId is not null)
	) as metnode 
	on bill_acc.bill_acc_Id = metnode.PersonalAccountId
	left join (
	select Id						        as bill_prem_Id, 
		   IbzkhId					        as bill_prem_IbzkhId,
		   Number					        as bill_prem_Number,
		   IsDeleted				        as bill_prem_isDeleted
	from BillingFund.dbo.PremiseInformations
	) as bill_prem
	on bill_prem.bill_prem_Id = metnode.PremiseInformationId;
------------------------------------------------------------------------------------------------------------------------------------------------------------
select rel.LegalEntityId           as rel_Supplier, 
	   rel.AlienPremiseAccountId   as rel_CkuAccId, 
	   accprem.*
into #zkh_relaccprem                            
from ibzkh.dbo.RelatedPremiseAccounts as rel
	left join (
	select zkh_acc.zkh_acc_Id, 
		   zkh_acc.zkh_acc_Number, 
		   zkh_acc.zkh_acc_isDeleted,
		   zkh_acc.zkh_acc_EndDate,
		   zkh_prem.*
	from (
		select Id				         as zkh_acc_Id,
			   PremiseInformationId,  
			   Number			         as zkh_acc_Number,
			   IsDeleted		         as zkh_acc_isDeleted,
			   EndDate                   as zkh_acc_EndDate
		from ibzkh.dbo.PremiseAccounts
		) as zkh_acc
		left join (
		select Id                     as zkh_prem_Id, 
			   Number			      as zkh_prem_Number, 
			   IsDeleted		      as zkh_prem_isDeleted 
		from ibzkh.dbo.PremiseInformations
		) as zkh_prem
		on zkh_acc.PremiseInformationId = zkh_prem.zkh_prem_Id
	) as accprem
	on rel.PremiseAccountId = accprem.zkh_acc_Id;
------------------------------------------------------------------------------------------------------------------------------------------------------------
select * 
into #bill_zkh
from #bill_accmetprem 
join #zkh_relaccprem 
on (bill_acc_IbzkhId = zkh_acc_Id and bill_prem_IbzkhId = zkh_prem_Id);
------------------------------------------------------------------------------------------------------------------------------------------------------------
drop table #bill_accmetprem, #zkh_relaccprem;
------------------------------------------------------------------------------------------------------------------------------------------------------------


	select '1 счета биллинга, у которых жкх_id помещения не совпадает со связанными с ними помещениями' as 'особенность', count(*) as 'количество' from #bill_zkh 
	where bill_acc_IbzkhPremId != bill_prem_IbzkhId
union all
	select '2 не совпал номер счета/помещения в биллинге и ибжкх', count(*) from #bill_zkh
	where bill_acc_Number != zkh_acc_Number or bill_prem_Number != zkh_prem_Number
union all
	select '3 отсутствует номер счета/помещения в биллинге и ибжкх', count(*) from #bill_zkh
	where zkh_acc_Number is null or zkh_prem_Number is null or
		  (bill_acc_Number is null and bill_acc_Id is not null) or 
		  (bill_prem_Number is null and bill_prem_Id is not null) 
union all
	select '4 не совпал статус удаления счета/помещения в биллинге и ибжкх', count(*) from #bill_zkh
	where bill_acc_isDeleted != zkh_acc_isDeleted or bill_prem_isDeleted != zkh_prem_isDeleted
union all
	select '5 не совпал статус закрытия счета в биллинге и ибжкх', count(*) from #bill_zkh
	where bill_acc_EndDate != zkh_acc_EndDate
union all
	select '6 узел учета или счет/помещение в биллинге/ибжкх удален', count(*) from #bill_zkh
	where zkh_acc_isDeleted = 1 or zkh_prem_isDeleted = 1 or
		  met_isDeleted = 1 or bill_acc_isDeleted = 1 or bill_prem_isDeleted = 1
union all 
	select '7 узел учета - не кап.ремонт, внешняя система - не цку', count(*) from #bill_zkh
	where (not (met_Supplier = 1 and met_ServiceId = 14)) or (rel_Supplier != 4733)
union all 
	select '8 связанное помещение/счет отсутствует в биллинге/ибжкх', count(*) from #bill_zkh
	where bill_acc_Id is null or bill_prem_Id is null or zkh_acc_Id is null or zkh_prem_Id is null
union all
	select '9 счета ИБЖКХ, к которым привязано несколько помещений', count(*)
	from (select zkh_acc_Id, count(*) zkhPremCount
	     from #bill_zkh
		 group by zkh_acc_Id
		 having count(*) > 1) as groups  -- по какой-то причине для одного счета имеется несколько строк (это условие учтет zkh_prem_id = NULL)
union all
	select '10 помещения, к которым привязано несколько счетов', count(*)
	from (select zkh_prem_Id, count(*) zkhAccCount
	     from #bill_zkh
		 group by zkh_prem_Id
		 having count(*) > 1) as premgroups  -- по какой-то причине для одного помещения имеется несколько строк (это условие учтет zkh_acc_id = NULL)
union all
	select '11 счета ЦКУ, к которым привязано несколько счетов ИБ ЖКХ', count(*)
	from (select #bill_zkh.rel_CkuAccId, count(*) zkhAccCount 
		 from #bill_zkh
		 group by #bill_zkh.rel_CkuAccId
		 having count(*) > 1) as ckuaccgroups


-------------------------------------------------------------------------------------------------------------------------------------------------------------------


declare @maxFileId int;
set @maxFileId = (select max(ImportFileInfoId) from ibzkh.dbo.ChargeExportCKUs);

select *
into #bill_file
from 
	(select rel_CkuAccId, 
		    bill_acc_Id, bill_acc_IbzkhId, bill_acc_Number,
		    bill_prem_Id, bill_prem_IbzkhId, bill_prem_Number,
			rel_Supplier
	from #bill_zkh
	where #bill_zkh.rel_Supplier = 4733 and
		  #bill_zkh.met_ServiceId = 14 and
		  #bill_zkh.met_Supplier = 1) as bill_accprem
	right join
	(select distinct LS_ID, LS, PREM_ID, KV, PER, ImportFileInfoId
	from ibzkh.dbo.ChargeExportCKUs
	where ImportFileInfoId = @maxFileId) as cku_file
	on bill_accprem.rel_CkuAccId = cku_file.LS_ID
-------------------------------------------------------------------------------------------------------------------------------------------------------------------	
drop table #bill_zkh
-------------------------------------------------------------------------------------------------------------------------------------------------------------------


select '1 - счет отсутствует в базе' as 'Bad Account', count(*) as 'Count'
from (select *
	 from #bill_file
	 where rel_CkuAccId is null) as absentAcc
union all
	select '2 - счет в базе, но в файле не заполнен номер', count(*) 
	from (select distinct bill_acc_Id
		 from #bill_file
		 where rel_CkuAccId is not null and LS = '') as emptyAcc
union all
	select '3 - отличается номер в файле и в базе', count(*)
	from (select distinct bill_acc_Id
		 from #bill_file
		 where rel_CkuAccId is not null and LS != '' and LS != bill_acc_Number) as differentAcc
union all
	select '4 - к одному счету ЦКУ привязано несколько счетов ИБ ЖКХ', count(*)
	from (select LS_ID
		 from #bill_file
		 where rel_CkuAccId is not null
		 group by LS_ID
		 having count(bill_acc_Id) > 1) as multipleAcc
-------------------------------------------------------------------------------------------------------------------------------------------------------------------


select LS_ID, bill_acc_Id, rel_Supplier, PREM_ID, bill_prem_Id 
from #bill_file
where rel_CkuAccId is not null and
      LS = bill_acc_Number and 
	  LS_ID not in (select LS_ID 
				   from #bill_file
				   where rel_CkuAccId is not null
				   group by LS_ID
				   having count(bill_acc_Id) > 1)

drop table #bill_file