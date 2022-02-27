select acc.Id accId, acc.Number accNumber, acc.IsDeleted accDeleted, 
	   prem.Id premId, prem.Number premNumber, prem.IsDeleted premDeleted
into #accprem
from ibzkh.dbo.PremiseAccounts as acc 
	left join (select Id, Number, IsDeleted from ibzkh.dbo.PremiseInformations) as prem
	on acc.PremiseInformationId = prem.Id;

select rel.AlienPremiseAccountId as ckuAccId,
	   #accprem.*,
	   rel.LegalEntityId as supplier
into #relaccprem                                   -- ������� RelatedAccouts.
from ibzkh.dbo.RelatedPremiseAccounts as rel
	left join #accprem 
	on rel.PremiseAccountId = #accprem.accId

select '���������� ������, ����������� � related, �� ��������� ������������� � ����' as comment,  count(*) as accCount from #relaccprem
where accId is null

union all
select '���������� ������ ���, � ������� ��������� ��������� ������ �� ���', count (*)
from (select ckuAccId, count(accId) as zkhacccount 
	 from #relaccprem
     where #relaccprem.supplier = 4733
     group by #relaccprem.ckuAccId
     having count(accId) > 1) as manycku
where manycku.zkhacccount > 1

union all
select '���������� ������ �� ���, � ������� ��������� ��������� ������ ���', count (*)
from (select accId, count(ckuAccId) as ckuacccount 
	 from #relaccprem
     where #relaccprem.supplier = 4733
     group by #relaccprem.accId
     having count(ckuAccId) > 1) as manycku
where manycku.ckuacccount > 1

union all
select '���������� ������, � �������� �� ������� ���������',  count(*) from #relaccprem
where premId is null

union all
select '���������� �������������� ������, � ������� �� �������� �����', count(*) from #relaccprem
where accDeleted = 0 and accNumber is null

union all
select '���������� ��� ��������� � ������, � ������� ���� � ������ ������� ���������� ��������', count(*) from #relaccprem
where accDeleted = 0 and premDeleted = 1 or
      accDeleted = 1 and premDeleted = 0

union all
select '���������� ������, � �������� ������� ��������� ���������', count(accgrps.premCounts) 
from (select count(premId) as premCounts 
	  from #relaccprem 
	  group by accId 
	  having count(premId) > 1) as accgrps;


drop table #accprem, #relaccprem;




	 -- and (#PremAcc.accDeleted = 0 and #PremAcc.premDeleted = 0 or #PremAcc.accDeleted = 1 and #PremAcc.premDeleted = 1);
