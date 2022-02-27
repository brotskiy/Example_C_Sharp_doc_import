select billacc.Id accId, billacc.IbzkhId ibzkhAccId, billacc.IbzkhPremiseInformationId ibzkhAccPremId, billacc.IsDeleted accDeleted, billacc.EndDate accEndDate,
	   metnode.Id metId, metnode.UtilityServiceId serviceId, metnode.SupplierId metSupplier, metnode.IsDeleted metDeleted, metnode.EndDate metEndDate,
	   billprem.Id premId, billprem.IbzkhId ibzkhPremId, billprem.IsDeleted premDeleted
into #billaccmetprem
from (select Id, IbzkhId, IbzkhPremiseInformationId, IsDeleted, EndDate from BillingFund.dbo.PersonalAccounts) as billacc
	 right join
	 (select Id, PersonalAccountId, PremiseInformationId, UtilityServiceId, SupplierId, IsDeleted, EndDate from BillingFund.dbo.MeteringNodes
	 where (PersonalAccountId is not null) and (PremiseInformationId is not null)) as metnode
	 on billacc.Id = metnode.PersonalAccountId
	 left join
     (select Id, IbzkhId, IsDeleted from BillingFund.dbo.PremiseInformations) as billprem
	 on billprem.Id = metnode.PremiseInformationId;

select '���������� ��, ��������� � �������������� ����������/������', count(*) from #billaccmetprem
where (accId is null) or (premId is null)

union all
select '���������� �������������� ��, ��������� � ����������������� ����������/������', count(*) from #billaccmetprem
where (metDeleted = 0) and
      (accDeleted = 0 and premDeleted = 1 or accDeleted = 1 and premDeleted = 0 or accDeleted = 1 and premDeleted = 1)

union all
select '���������� ��� ������ � ��, � ������� ����� ����� ������� �������� ��������', count(*) from #billaccmetprem
where (accEndDate is null) and (metEndDate is not null) or
      (accEndDate is not null) and (metEndDate is null)
	   
union all
select '���������� ��, ���������� �����������', count(*)
from (select count(distinct metId) as nodesCount 
	 from #billaccmetprem
	 where (metDeleted = 0) and (metEndDate is null) and    -- �������������� ���������� ��,
	       (accId is not null) and (premId is not null)     -- ��������� � ���������� � ������.
     group by accId, premId, serviceId, metSupplier
     having count(distinct metId) > 1) as duplicateNodes

union all
select '���������� ���������, ����������� � ���������� ������', count(*)
from (select accId, count(distinct premId) as premisesCount 
	 from #billaccmetprem
	 where (metDeleted = 0) and (metEndDate is null) and    -- �������������� ���������� ��,
	       (accId is not null) and (premId is not null)     -- ��������� � ���������� � ������. (�������� acc/premDeleted)
	 group by accId
	 having count(distinct premId) > 1) as multiplePremises

union all
select '���������� ������, ����������� � ���������� ����������', count(*)
from (select premId, count(distinct accId) as accountsCount 
	 from #billaccmetprem
	 where (metDeleted = 0) and (metEndDate is null) and    -- �������������� ���������� ��,
	       (accId is not null) and (premId is not null)     -- ��������� � ���������� � ������. (�������� acc/premDeleted)
	 group by premId
	 having count(distinct accId) > 1) as multipleAccounts


drop table #billaccmetprem;