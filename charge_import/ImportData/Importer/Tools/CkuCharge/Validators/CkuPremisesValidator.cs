using System;
using System.Collections.Generic;
using System.Linq;
using AddressService.NuGet;
using AddressService.NuGet.Utility.Enums;
using AddressService.NuGet.CacheUnits.Interfaces;
using BillingServiceBus.OverhaulServiceGateway;
using BillingServiceBus.PremiseServiceReference;
using Import.API.Tools.ImportData.Importer.Tools.CkuCharge.Comparers;

namespace Import.API.Tools.ImportData.Importer.Tools.CkuCharge.Validators
{

public class CkuPremisesValidator
{
// :::::::::::::::::::::::::::::::::::::::: Содержимое. ::::::::::::::::::::::::::::::::::::::::

public enum ValidationResult : int
{   
    ok_FullyContained,             // помещения, содержащиеся в таблице соответствий и в базе.
    ok_Fresh,                      // абсолютно новое помещение.

    fail_TableNotDbContained,      // помещение с записью в таблице соответствий, но отсутствующее в базе.
    fail_deletedPremises,          // помещение, помеченное в базе как удаленное.
    fail_notInOverhaul,            // помещение, содержащееся в доме, который не подлежит кап. ремонту.
    fail_invalidFiasId,            // помещение, дому которого присвоен неверный ФИАС.
    fail_unknownFiasId,            // помещение, дом которого имеет неизвестный нам ФИАС.
    fail_addressMissmatch,         // помещение, в записях которого одному id ЦКУ соответствуют несколько адресов помещений.
    fail_CkuIdMissmatch,           // помещение, в записях которого одному адресу помещений соответствуют несколько id ЦКУ.
    fail_BuildAddressMissmatch,    // помещение, в записях которого одному ФИАС-у соответствую несколько адресов домов.
    fail_FiasMissmatch             // помещение, в записях которого одному адресу дома соответствую несколько ФИАС-ов.
}

private readonly IAddressService _addressService;
private readonly IPremiseService _premisesService;
private readonly IOverhaulServiceGateway _overhaulService;

// :::::::::::::::::::::::::::::::::::::::: Создание. ::::::::::::::::::::::::::::::::::::::::

public CkuPremisesValidator(IOverhaulServiceGateway overhaulService, IPremiseService premiseService)
{
    _addressService  = AddressServiceFactory.Default;
    _premisesService = premiseService;
    _overhaulService = overhaulService;
}

// :::::::::::::::::::::::::::::::::::::::: Работа. ::::::::::::::::::::::::::::::::::::::::

public Dictionary<ValidationResult, IEnumerable<CkuPremisesDetail>> Validate(IEnumerable<CkuPremisesDetail> incomingDetails)
{
    const int ckuSupplierId       = 4733;    // id поставщика данных "ЦКУ города Шахты".
    const int shakhtySettlementId = 21;      // id муниципального образования "город Шахты".

    var documentValidationResult = ValidateDocument(incomingDetails);
    var result                   = new Dictionary<ValidationResult, IEnumerable<CkuPremisesDetail>>();
    incomingDetails              = documentValidationResult.ConsistentDetails;

    foreach (var pair in documentValidationResult.FailedDetails)
        Miscellanea.InsertOrAdd(result, pair.Key, pair.Value);    

    // получаем таблицу соотвествий id шахтинских помещений у нас и ЦКУ.
    var MATCHING_TABLE = _premisesService
        .GetPremiseMatches(ckuSupplierId)
        .ToDictionary(dto => dto.ExternId);

    // получаем ВСЕ шахтинские дома, хранящиеся в базе.
    var shakhtyHouses = _addressService.Formations
        .GetHouses(shakhtySettlementId, HouseFilter.All)
        .GroupBy(house => house.BuildingId, (key, group) => 
        {
            var housesWithFias      = group.Where(house => house.FiasId.HasValue);
            int nontrivialFiasCount = housesWithFias.Select(house => house.FiasId.Value).Distinct().Count();    // количество различных ненулевых фиасов для данного id дома.

            switch (nontrivialFiasCount)
            {
                case 0:
                    return group.First();             // если ни у одного адреса данного дома не проставлен ФИАС, то вернем любой адрес.
                case 1:
                    return housesWithFias.First();    // если у адресов данного дома с заполненным ФИАС-ом его значение единственно, то вернем любой из этих адресов.
                default:
                    throw new Exception($"Дому с buildingId={key} соответствуют адреса с различными fiasId");    // ошибка: одному дому не могут соответствовать несколько ФИАС-ов.
            }
        })
        .ToList();
    
    // для ВСЕХ шахтинских домов получаем имеющиеся в базе помещения.
    var SHAKHTY_PREMISES = _premisesService
        .GetPremisesByBuildingIdsLargeCollection(shakhtyHouses.Select(house => house.BuildingId).ToArray())
        .Join(shakhtyHouses, prem => prem.BuildingId, house => house.BuildingId, (prem, house) => 
        new PremiseHouse
        { 
            Premise = prem,
            House   = house
        })
        .ToList();

    // получаем id шахтинских домов, которые когда-либо подлежали кап. ремонту.
    var SHAKHTY_OVERHAUL_BUILDINGIDS = new HashSet<int>(_overhaulService
        .GetBySettlement(shakhtySettlementId)
        .Where(build => build.InOverhaulSchedule || build.WasInOverhaulSchedule)
        .Select(build => build.Buildingd));

    // разделяем пришедшие помещения по их наличию в таблице соответствий.
    var incomingInTable = Miscellanea.
        Filter(incomingDetails, prem => MATCHING_TABLE.ContainsKey(prem.CkuId), out var incomingNotInTable);

    var inTableResult    = ValidateInTable(incomingInTable, MATCHING_TABLE, SHAKHTY_PREMISES, SHAKHTY_OVERHAUL_BUILDINGIDS);
    var notInTableResult = ValidateNotInTable(incomingNotInTable, MATCHING_TABLE, SHAKHTY_PREMISES, SHAKHTY_OVERHAUL_BUILDINGIDS);
        
    foreach (var pair in inTableResult)
        Miscellanea.InsertOrAdd(result, pair.Key, pair.Value); 

    foreach (var pair in notInTableResult)
        Miscellanea.InsertOrAdd(result, pair.Key, pair.Value); 

    return result;
}

private DocumentValidationResult ValidateDocument(IEnumerable<CkuPremisesDetail> incomingDetails)
{
    bool IsEmptyBuildAddress(CkuPremisesDetail prem)
    {
        return string.IsNullOrWhiteSpace(prem.UL_TP)
            && string.IsNullOrWhiteSpace(prem.UL)
            && string.IsNullOrWhiteSpace(prem.DOM)
            && string.IsNullOrWhiteSpace(prem.KOR);
    }

    bool IsEmptyAddress(CkuPremisesDetail prem)
    {
        return IsEmptyBuildAddress(prem) && string.IsNullOrWhiteSpace(prem.KV);
    }

    bool IsNonInvalidFias(CkuPremisesDetail prem)
    {
        return string.IsNullOrWhiteSpace(prem.FiasId) || Guid.TryParse(prem.FiasId, out Guid parsed);    // ФИАС НЕ является битым, если он пуст, либо его можно распарсить.
    }

    // среди пришедших оставили только помещения, у которых НЕ битый ФИАС.
    incomingDetails = Miscellanea
        .Filter(incomingDetails, IsNonInvalidFias, out var invalidFias);
    
    // 1).
    // сначала среди пришедших оставили записи только тех помещений, у которых для id ЦКУ однозначен адрес помещения.
    var comparers   = new List<IEqualityComparer<CkuPremisesDetail>> { new PremiseAddressComparer() };
    incomingDetails = Miscellanea
        .GetFactorizablyEquivalent(incomingDetails, prem => prem.CkuId, comparers, out var invalidAddressMissmatch);



    /*
    // 2).   
    // временно отложим записи с пустым адресом помещения, т.к. он будет выступать в роли ключа для группировки.
    incomingDetails = Miscellanea
        .Filter(incomingDetails, prem => !IsEmptyAddress(prem), out var emptyAddr);

    // затем среди пришедших оставили записи только тех помещений, у которых для адреса помещения однозначен id ЦКУ.
    comparers       = new List<IEqualityComparer<CkuPremisesDetail>> { new PremiseCkuIdComparer() };
    incomingDetails = Miscellanea
        .GetFactorizablyEquivalent(incomingDetails, 
                                   prem => (prem.UL_TP, prem.UL, prem.DOM, prem.KOR, prem.KV), 
                                   comparers, 
                                   out var invalidCkuIdMissmatch);
    
    // вернем записи с пустым адресом назад к набору анализируемых.
    incomingDetails = incomingDetails.Concat(emptyAddr).ToList();
    */
    IEnumerable<CkuPremisesDetail> invalidCkuIdMissmatch = new List<CkuPremisesDetail>();



    // 3).    
    // временно отложим записи с пустым ФИАС, т.к. он будет выступать в роли ключа для группировки.        
    incomingDetails = Miscellanea
        .Filter(incomingDetails, prem => !string.IsNullOrWhiteSpace(prem.FiasId), out var emptyFias);

    // затем среди пришедших оставили записи только тех помещений, у которых для ФИАС-а однозначен адрес ДОМА.
    comparers       = new List<IEqualityComparer<CkuPremisesDetail>> { new PremiseBuildingAddressComparer() };
    incomingDetails = Miscellanea
        .GetFactorizablyEquivalent(incomingDetails, prem => prem.FiasId, comparers, out var invalidBuildAddressMissmatch);

    // вернем записи с пустым ФИАС назад к набору анализируемых.
    incomingDetails = incomingDetails.Concat(emptyFias).ToList();

    // 4).    
    // временно отложим записи с пустым адресом ДОМА, т.к. он будет выступать в роли ключа для группировки.        
    incomingDetails = Miscellanea.Filter(incomingDetails, prem => !IsEmptyBuildAddress(prem), out var emptyBuildAddr);

    // затем среди пришедших оставили записи только тех помещений, у которых для адреса ДОМА однозначен ФИАС.
    comparers       = new List<IEqualityComparer<CkuPremisesDetail>> { new PremiseFiasComparer() };
    incomingDetails = Miscellanea
        .GetFactorizablyEquivalent(incomingDetails, 
                                   prem => (prem.UL_TP, prem.UL, prem.DOM, prem.KOR), 
                                   comparers, 
                                   out var invalidFiasMissmatch);

    // вернем записи с пустым адресом ДОМА назад к набору анализируемых.
    incomingDetails = incomingDetails.Concat(emptyBuildAddr).ToList();

    var result = new DocumentValidationResult()
    {
        ConsistentDetails = incomingDetails,
    };

    if (invalidFias.Any())
        result.FailedDetails[ValidationResult.fail_invalidFiasId] = invalidFias;

    if (invalidAddressMissmatch.Any())
        result.FailedDetails[ValidationResult.fail_addressMissmatch] = invalidAddressMissmatch;

    if (invalidCkuIdMissmatch.Any())
        result.FailedDetails[ValidationResult.fail_CkuIdMissmatch] = invalidCkuIdMissmatch;

    if (invalidBuildAddressMissmatch.Any())
        result.FailedDetails[ValidationResult.fail_BuildAddressMissmatch] = invalidBuildAddressMissmatch;

    if (invalidFiasMissmatch.Any())
        result.FailedDetails[ValidationResult.fail_FiasMissmatch] = invalidFiasMissmatch;


    var ERRORS = invalidCkuIdMissmatch.Select(det => det.CkuId).Distinct().ToArray();

    return result;
}

private Dictionary<ValidationResult, IEnumerable<CkuPremisesDetail>> ValidateInTable(IEnumerable<CkuPremisesDetail> incomingDetails,
                                                                                     Dictionary<int, PremiseMatchDTO> MATCHING_TABLE,
                                                                                     IEnumerable<PremiseHouse> SHAKHTY_PREMISES,
                                                                                     HashSet<int> SHAKHTY_OVERHAUL_BUILDINGIDS)
{
    // из ШАХТИНСКИХ помещений в БАЗЕ выделяем те, для которых существует запись в таблице соответствий id ЦКУ-ИБЖКХ.
    var ckuPremises = SHAKHTY_PREMISES
        .Join(MATCHING_TABLE, prem => prem.Premise.Id, match => match.Value.ZkhId, (prem, match) => 
        new
        {
            BillingId      = prem.Premise.Id,
            CkuId          = match.Value.ExternId,
            BuildingId     = prem.Premise.BuildingId,
            BuildingFiasId = prem.House.FiasId,
            IsDeleted      = prem.Premise.IsDeleted
        })
        .ToDictionary(prem => prem.CkuId);
        
    // среди ПРИШЕДШИХ оставляем только те помещения с записью в таблице соответствий, которые реально хранятся в базе.
    incomingDetails = Miscellanea
        .Filter(incomingDetails, prem => ckuPremises.ContainsKey(prem.CkuId), out var invalidInTableNotInDb);

    // среди пришедших оставляем только действующие помещения.
    incomingDetails = Miscellanea
        .Filter(incomingDetails, prem => !ckuPremises[prem.CkuId].IsDeleted, out var invalidDeleted);

    // среди пришедших оставляем только помещения, содержащиеся в домах, которые входят в программу кап. ремонта.
    incomingDetails = Miscellanea
        .Filter(incomingDetails, prem => SHAKHTY_OVERHAUL_BUILDINGIDS.Contains(ckuPremises[prem.CkuId].BuildingId), out var invalidNotInOverhaul);

    bool CheckFias(CkuPremisesDetail prem)
    {
        Guid? storedFias = ckuPremises[prem.CkuId].BuildingFiasId;             // получаем хранящийся в базе ФИАС дома, содержащего данное помещение.

        if (!storedFias.HasValue || string.IsNullOrWhiteSpace(prem.FiasId))    // если у хранящегося дома не заполнено поле ФИАС, или пришел пустой ФИАС.
            return true;                                                       // нельзя сравнить пришедший ФИАС с хранящимся, поэтому принимаем любое его значение.
    
        Guid.TryParse(prem.FiasId, out Guid incomingFias);    // правильность ФИАС-а уже проверена выше.
        return incomingFias == storedFias.Value;
    }

    // среди пришедших оставляем помещения с верными ФИАС id содержащих их домов.
    incomingDetails = Miscellanea
        .Filter(incomingDetails, CheckFias, out var invalidWrongFias);

    // у помещений в базе и в таблице соответствий заполняем их id в системе биллинга и номер их дома.
    foreach (CkuPremisesDetail prem in incomingDetails)
        prem.Stored = new CkuPremisesAddInfo
        {
            PremisesId = MATCHING_TABLE[prem.CkuId].ZkhId,
            BuildingId = ckuPremises[prem.CkuId].BuildingId
        };

    var result = new Dictionary<ValidationResult, IEnumerable<CkuPremisesDetail>>();

    if (incomingDetails.Any())
        result[ValidationResult.ok_FullyContained] = incomingDetails;

    if (invalidInTableNotInDb.Any())
        result[ValidationResult.fail_TableNotDbContained] = invalidInTableNotInDb;

    if (invalidDeleted.Any())
        result[ValidationResult.fail_deletedPremises] = invalidDeleted;

    if (invalidNotInOverhaul.Any())
        result[ValidationResult.fail_notInOverhaul] = invalidNotInOverhaul;

    if (invalidWrongFias.Any())
        result[ValidationResult.fail_invalidFiasId] = invalidWrongFias;

    return result;
}

private Dictionary<ValidationResult, IEnumerable<CkuPremisesDetail>> ValidateNotInTable(IEnumerable<CkuPremisesDetail> incomingDetails,
                                                                                        Dictionary<int, PremiseMatchDTO> MATCHING_TABLE, 
                                                                                        IEnumerable<PremiseHouse> SHAKHTY_PREMISES,
                                                                                        HashSet<int> SHAKHTY_OVERHAUL_BUILDINGIDS)
{
    // из пришедших отфильтруем помещения с пустым ФИАС, т.к. для них невозможно определить содержащий их дом.
    incomingDetails = Miscellanea
        .Filter(incomingDetails, prem => !string.IsNullOrWhiteSpace(prem.FiasId), out var invalidFias);

    // получим ФИАС-ы шахтинских домов, подлежащих кап.ремонту.
    var shakhtyOverhaulFiasIds = SHAKHTY_PREMISES
        .Where(prem => prem.House.FiasId.HasValue)                                      // от записей, в которых не указан ФИАС, нам не будет никакого толку.
        .Where(prem => SHAKHTY_OVERHAUL_BUILDINGIDS.Contains(prem.House.BuildingId))    // оставляем только записи домов, подлежащих кап.ремонту.
        .GroupBy(prem => prem.House.FiasId.Value,
        (key, group) => 
        new 
        { 
            Fias  = key, 
            Build = group.Select(prem => prem.House.BuildingId).Distinct().Single()     // согласно программе кап.ремонта ФИАС и id дома взаимнооднозначны!
        })
        .ToDictionary(pair => pair.Fias, pair => pair.Build);

    // отдельно получим ФИАС-ы шахтинских домов из базы, НЕ подлежащих кап.ремонту!
    var shakhtyNotOverhaulFiasIds = new HashSet<Guid>(SHAKHTY_PREMISES
        .Where(prem => prem.House.FiasId.HasValue && !shakhtyOverhaulFiasIds.ContainsKey(prem.House.FiasId.Value))
        .Select(prem => prem.House.FiasId.Value)
        .Distinct());
      
    // получить ФИАС по его строковому представлению. успех гарантирован, т.к. пустые и неправильные строки уже отфильтрованы.
    Guid Fias(CkuPremisesDetail prem)
    {
        Guid.TryParse(prem.FiasId, out Guid incomingFias);
        return incomingFias;
    }

    // среди пришедших оставляем только помещения, содержащиеся в подлежащих кап.ремонту домах.
    incomingDetails = Miscellanea
        .Filter(incomingDetails, prem => shakhtyOverhaulFiasIds.ContainsKey(Fias(prem)), out var invalidNotInOverhaul);

    // среди отбракованных дополнительно выделим помещения, которые вообще не содержатся в базе.
    invalidNotInOverhaul = Miscellanea
        .Filter(invalidNotInOverhaul, prem => shakhtyNotOverhaulFiasIds.Contains(Fias(prem)), out var invalidUnknownFias);

    // у новых помещений будем заполнять номер дома, в который они добавляются.
    foreach (CkuPremisesDetail prem in incomingDetails)
        prem.Stored = new CkuPremisesAddInfo
        {
            PremisesId = null,
            BuildingId = shakhtyOverhaulFiasIds[Fias(prem)]
        };

    var result = new Dictionary<ValidationResult, IEnumerable<CkuPremisesDetail>>();

    if (incomingDetails.Any())
        result[ValidationResult.ok_Fresh] = incomingDetails;
    
    if (invalidFias.Any())
        result[ValidationResult.fail_invalidFiasId] = invalidFias;

    if (invalidNotInOverhaul.Any())
        result[ValidationResult.fail_notInOverhaul] = invalidNotInOverhaul;

    if (invalidUnknownFias.Any())
        result[ValidationResult.fail_unknownFiasId] = invalidUnknownFias;



    var ERROR_FIAS = invalidUnknownFias
        .Select(prem => prem.FiasId)
        .Distinct()
        .ToArray();



    return result;
}

// :::::::::::::::::::::::::::::::::::::::: Полезности. ::::::::::::::::::::::::::::::::::::::::

public static bool IsError(ValidationResult result)
{
    return result != ValidationResult.ok_FullyContained &&
           result != ValidationResult.ok_Fresh;
}

private class DocumentValidationResult
{
    public DocumentValidationResult()
    {
        ConsistentDetails = new List<CkuPremisesDetail>();
        FailedDetails     = new Dictionary<ValidationResult, IEnumerable<CkuPremisesDetail>>();
    }

    public IEnumerable<CkuPremisesDetail> ConsistentDetails { get; set; }                              // записи документа, подлежащие дальнейшей обработке.
    public Dictionary<ValidationResult, IEnumerable<CkuPremisesDetail>> FailedDetails { get; set; }    // записи документа с ошибками уже после начальной проверки.
}

private class PremiseHouse
{
    public PremiseDTO Premise { get; set; }   
    public IHouse House { get; set; }
}

}

}