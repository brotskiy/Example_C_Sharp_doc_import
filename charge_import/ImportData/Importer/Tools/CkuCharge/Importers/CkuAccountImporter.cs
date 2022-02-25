using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BillingServiceBus.BillingServiceReference;
using BillingServiceBus.PersonalAccountServiceReference;
using BillingServiceBus.MeteringNodeServiceReference;
using IbZKH_CustomTypes.SingleTypes;
using Tools.Extensions;

namespace Import.API.Tools.ImportData.Importer.Tools.CkuCharge.Importers
{

public class CkuAccountImporter
{
// :::::::::::::::::::::::::::::::::::::::: Содержимое. ::::::::::::::::::::::::::::::::::::::::

private const int OverhaulServiceId     = (int)UtilityServiceEnum.Overhaul;    // id услуги типа "Капитальный ремонт".
private const int OverhaulSupplierId    = 1;                                   // id поставщика "Фонд кап.ремонта".
private const int GeneralActivityTypeId = 1;                                   // id общего типа деятельности.
private const int CkuOrganizationId     = 4733;

private readonly IBillingService _billingService;
private readonly IPersonalAccountService _accountService;

// :::::::::::::::::::::::::::::::::::::::: Создание. ::::::::::::::::::::::::::::::::::::::::

public CkuAccountImporter(IBillingService billingService, 
                          IPersonalAccountService accountService)
{
    _billingService      = billingService;
    _accountService      = accountService;
}

// :::::::::::::::::::::::::::::::::::::::: Работа. ::::::::::::::::::::::::::::::::::::::::

public IEnumerable<CkuAccountImportDTO> ImportNewAccounts(IEnumerable<CkuAccountImportDTO> accountsToAdd_out)
{
    try
    {
        var (regionalTariff, specialTariffs) = SelectOverhaulTariffs();
        var errors = new List<string>(); 
        
        foreach (CkuAccountImportDTO acc in accountsToAdd_out)
        {
            try    // если на каком-то этапе конвейера происходит ошибка, то переходим к следующему счету, выбросив исключение.
            {
                AddNewAccountIntoDb(acc, regionalTariff, specialTariffs);    // добавляем в базу новый счет.
                AddAccountIntoMatchingTable(acc.CkuId, acc.Id.Value);        // добавляем счет в таблицу соответствий.
            }
            catch (Exception ex)
            {
                errors.Add(ex.GetFullExceptionMessage());
            }
        }

        if (errors.Any())
            throw new Exception(string.Join(Environment.NewLine, errors));

        return accountsToAdd_out;
    }
    catch (Exception ex)
    {
        throw new Exception($"При импорте счетов произошла ошибка!{Environment.NewLine}{ex.GetFullExceptionMessage()}");
    }
}

private CkuAccountImportDTO AddNewAccountIntoDb(CkuAccountImportDTO acc_out,
                                                UtilityTariffFullDTO regionalTariff, 
                                                Dictionary<int, UtilityTariffFullDTO> specialTariffs)
{
    try
    {
        // по записи счета в документе создаем объект указанного для него тарифа кап.ремонта.
        var storedTariff = GetTariff(acc_out, regionalTariff, specialTariffs);    

        // проверяем параметры тарифа, считанные из документа.
        OperationResult IsValidTariff = CheckTariff(storedTariff.CurentVolume.Value, acc_out.MeteringNode.OverhaulTariff);       
        if (IsValidTariff.HasError)
            throw new Exception(IsValidTariff.GetErrors());

        // по записи счета в документе создаем его узел учета. 
        var nodeDto = GetMeteringNode(acc_out, storedTariff);     
        var accDto  = new PersonalAccountSetDTO
        {
            BeginDate             = acc_out.BeginDate,
            PayerPhysicalPersonId = acc_out.PayerPhysicalPersonId
        };

        var accountId = Task.Run(() => _accountService.AddPersonalAccountForImport(accDto, acc_out.PeriodId, acc_out.PeriodId, null, new MeteringNodeSetDTO[1]{nodeDto}));
        Task.WaitAll(accountId);
        if (accountId.Result.HasError)
            throw new Exception(accountId.Result.GetErrors());

        // заполняем id счета полученным значением.
        acc_out.Id = accountId.Result.Result.Id;

        // заполняем id узла учета полученным значением.
        acc_out.MeteringNode.Id = _accountService
            .GetPersonalAccountNodesBaseCollection(new PersonalAccountIdSpecification { PersonalAccountId = acc_out.Id.Value})
            .Single()
            .MeteringNodeIdCollection
            .Single();

        // возвращаем счет с заполненными идентификаторами.
        return acc_out;
    }
    catch (Exception ex)
    {
        throw new Exception(ToMessage(acc_out, ex.GetFullExceptionMessage()));
    }
}

public void AddAccountIntoMatchingTable(int ckuId, int billlingId)
{
    try
    {
        var dto = new PersonalAccountMatchDTO
        {
            PersonalAccountId = billlingId,
            ExternId          = ckuId,
            SupplierId        = CkuOrganizationId
        };

        OperationResult wasAdded = _accountService.AddPersonalAccountMatch(dto);
        if (wasAdded.HasError)
            throw new Exception(wasAdded.GetErrors());
    }
    catch (Exception ex)
    {
        throw new Exception(ToMessage(ckuId, ex.GetFullExceptionMessage()));
    }
}

public IEnumerable<CkuAccountImportDTO> BindAccountsAndPremises(IEnumerable<CkuAccountImportDTO> accountsToBind)
{
    try
    {
        var (regionalTariff, specialTariffs) = SelectOverhaulTariffs();
        var errors = new List<string>();

        foreach (CkuAccountImportDTO acc in accountsToBind)
        {
            try
            {
                BindAccountAndPremises(acc, regionalTariff, specialTariffs);
            }
            catch (Exception ex)
            {
                errors.Add(ex.GetFullExceptionMessage());
            }
        }

        if (errors.Any())
            throw new Exception(string.Join(Environment.NewLine, errors));

        return accountsToBind;
    }
    catch (Exception ex)
    {
        throw new Exception($"При связывании счета и помещения произошла ошибка!{Environment.NewLine}{ex.GetFullExceptionMessage()}");
    }
}

private CkuAccountImportDTO BindAccountAndPremises(CkuAccountImportDTO acc_out,
                                                   UtilityTariffFullDTO regionalTariff, 
                                                   Dictionary<int, UtilityTariffFullDTO> specialTariffs)
{
    try
    {
        // по записи счета в документе создаем объект указанного для него тарифа кап.ремонта.
        var storedTariff = GetTariff(acc_out, regionalTariff, specialTariffs);

        // проверяем параметры тарифа, считанные из документа.
        OperationResult IsValidTariff = CheckTariff(storedTariff.CurentVolume.Value, acc_out.MeteringNode.OverhaulTariff);       
        if (IsValidTariff.HasError)
            throw new Exception($"При проверке тарифа для счета с цку id={acc_out.CkuId} произошла ошибка: {IsValidTariff.GetErrors()}");

        // по записи счета в документе создаем его узел учета. 
        var nodeDto = GetMeteringNode(acc_out, storedTariff); 
        var nodeId  = Task.Run(() => _accountService.AddMeteringNode(nodeDto));
        Task.WaitAll(nodeId);

        if (nodeId.Result.HasError)
            throw new Exception(nodeId.Result.GetErrors());

        acc_out.MeteringNode.Id = nodeId.Result.Result;

        return acc_out;
    }
    catch (Exception ex)
    {
        throw new Exception(ToMessage(acc_out, ex.GetFullExceptionMessage()));
    }
}

// :::::::::::::::::::::::::::::::::::::::: Полезности. ::::::::::::::::::::::::::::::::::::::::

private (UtilityTariffFullDTO regional, Dictionary<int, UtilityTariffFullDTO> specials) SelectOverhaulTariffs()
{
    // получаем все имеющиеся тарифы на кап.ремонт.
    var overhaulTariffs = _billingService
        .GetUtilityTariffs()
        .Where(tariff => tariff.UtilityService.Id == OverhaulServiceId && tariff.SupplierId == OverhaulSupplierId)
        .ToArray();
    
    // отдельно выбираем тарифы для специальных домов.
    var specialBuildingsTariffs = overhaulTariffs
        .Where(tariff => tariff.Building != null)
        .ToDictionary(tariff => tariff.Building.Id);
    
    // выбираем общий областной тариф кап.ремонта.
    var regionalTariff = overhaulTariffs
        .Where(tariff => tariff.Building == null)
        .Single();

    return (regionalTariff, specialBuildingsTariffs);
}

private static UtilityTariffFullDTO GetTariff(CkuAccountImportDTO acc, 
                                       UtilityTariffFullDTO regionalTariff, 
                                       Dictionary<int, UtilityTariffFullDTO> specialTariffs)
{
    var tariff = specialTariffs.ContainsKey(acc.Premises.BuildingId)
        ? specialTariffs[acc.Premises.BuildingId]
        : regionalTariff;

    return tariff;
}

private static OperationResult CheckTariff(decimal storedPrice, decimal incomingPrice)
{
    const decimal EPSILON = 1e-4M;

    var result = OperationResult.CreateSuccessResult(); 

    if (Math.Abs(incomingPrice - storedPrice) > EPSILON)
    {
        string message = $"Пришедшая величина тарифа кап.ремонта {incomingPrice} отличается от имеющегося в базе значения {storedPrice}.";
        result.AddError(message);
    }

    return result;
}

private static MeteringNodeSetDTO GetMeteringNode(CkuAccountImportDTO acc, UtilityTariffFullDTO tariff)
{
    return new MeteringNodeSetDTO
    {
        BeginDate                 = acc.MeteringNode.BeginDate,
        BeginInputDate            = acc.MeteringNode.BeginInputDate,
        SupplierId                = OverhaulSupplierId,
        IsSplittedPersonalAccount = false,
        CustomerActivityTypeId    = GeneralActivityTypeId,    // потребители услуги кап.ремонт имеют общий вид деятельности.
        UtilityServiceId          = OverhaulServiceId,
        TarriffId                 = tariff.Id,
        PersonalAccountId         = acc.Id,
        LinqToObject = new LinqToObjectDTO                    // узел учета устанавливается в помещении.
        {
            ObjectType = LinqObjectEnum.PremiseInformation,
            ObjectId = new GuidValueDTO { Value = acc.Premises.Id.Value}
        },
        ExpressionId               = 1,                   
        UtilityNormId              = null,                                  // у кап.ремонта нету нормы потребления.               
        CalculatingParameterValues = new CalculatingParameterBaseDTO[]{},    
        OwnershipParamaterValue    = new OwnershipParameterValueSetDTO 
        { 
            LegalEntityOwners    = new int[]{},
            PhysicalPersonOwners = new Guid[] { acc.Premises.Owner.Id.Value }  
        }
    };
}

private static string ToMessage(CkuAccountImportDTO acc, string what)
{
    return $"Счет: {acc.CkuId}, Ошибка: {what}";
}

private static string ToMessage(int ckuId, string what)
{
    return $"Счет: {ckuId}, Ошибка: {what}";
}

}

}