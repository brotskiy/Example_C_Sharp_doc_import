using System.Collections.Generic;
using System.Linq;
using BillingServiceBus.PersonalAccountServiceReference;
using AddressService.NuGet;
using Import.API.Tools.ImportData.Importer.Tools.CkuCharge.Comparers;

namespace Import.API.Tools.ImportData.Importer.Tools.CkuCharge.Validators
{

public class CkuAccountValidator
{ 
// :::::::::::::::::::::::::::::::::::::::: Содержимое. ::::::::::::::::::::::::::::::::::::::::

public enum ValidationResult : int
{   
    ok_FullyContained,           // счета, содержащиеся в таблице соответствий и в базе.
    ok_DbContained,              // счета, содержащиеся только в базе, но не в таблице соответствий.
    ok_Fresh,                    // абсолютно новый счет.

    fail_TableNotDbContained,    // счет с записью в таблице соответствий, но отсутствующий в базе.
    fail_ClosedAccount,          // счет, являющийся закрытым в базе.
    fail_MissmatchedNumber,      // счет, номер которого в таблице отличается от его номера в базе.
    fail_UnknownNumber,          // счет с неизвестным номером.
    fail_WrongNumber,            // счет c чужим номером.
    fail_numberMissmatch,        // счет, в записях которого одному id ЦКУ соответствует несколько номеров.
    fail_CkuIdMissmatch          // счет, в записях которого одному номеру соответсвует несколько id ЦКУ.
}

private readonly IAddressService _addressService;
private readonly IPersonalAccountService _personalAccountService;

// :::::::::::::::::::::::::::::::::::::::: Создание. ::::::::::::::::::::::::::::::::::::::::

public CkuAccountValidator(IPersonalAccountService personalAccountService)
{
    _addressService         = AddressServiceFactory.Default;
    _personalAccountService = personalAccountService;
}

// :::::::::::::::::::::::::::::::::::::::: Работа. ::::::::::::::::::::::::::::::::::::::::

public Dictionary<ValidationResult, IEnumerable<CkuAccountDetail>> Validate(IEnumerable<CkuAccountDetail> incomingDetails)
{
    const int ckuSupplierId = 4733;    // id поставщика данных "ЦКУ города Шахты".

    var documentValidationResult = ValidateDocument(incomingDetails);
    var result                   = new Dictionary<ValidationResult,IEnumerable<CkuAccountDetail>>();
    incomingDetails              = documentValidationResult.ConsistentDetails;
    
    foreach (var pair in documentValidationResult.FailedDetails)
        Miscellanea.InsertOrAdd(result, pair.Key, pair.Value);

    // получаем таблицу соотвествий id шахтинских счетов у нас и ЦКУ.
    var matchingTable = _personalAccountService
        .GetPersonalAccountMatches(ckuSupplierId)
        .ToDictionary(dto => dto.ExternId);

    // из пришедших выбираем счета с записью в таблице соответствий id ЦКУ-ИБЖКХ.
    var incomingDetailsInTable = incomingDetails
        .Where(acc => matchingTable.ContainsKey(acc.CkuId))
        .ToList();
    var inTableResult = ValidateInTable(incomingDetailsInTable, matchingTable);

    // из пришедших выбираем счета, не содержащиеся таблице соответствий id ЦКУ-ИБЖКХ.
    var incomingDetailsNotInTable = incomingDetails
        .Where(acc => !matchingTable.ContainsKey(acc.CkuId))
        .ToList();
    var notInTableResult = ValidateNotInTable(incomingDetailsNotInTable, matchingTable);

    foreach (var pair in inTableResult)
        Miscellanea.InsertOrAdd(result, pair.Key, pair.Value);

    foreach (var pair in notInTableResult)
        Miscellanea.InsertOrAdd(result, pair.Key, pair.Value);

    return result;    
}

private DocumentValidationResult ValidateDocument(IEnumerable<CkuAccountDetail> incomingDetails)
{
    // 1).
    // сначала среди пришедших оставили записи только тех счетов, у которых для id ЦКУ однозначен номер счета.
    var comparers   = new List<IEqualityComparer<CkuAccountDetail>>{ new AccountNumberComparer() };
    incomingDetails = Miscellanea
        .GetFactorizablyEquivalent(incomingDetails, acc => acc.CkuId, comparers, out var invalidNumberMissmatch);



    /*
    // 2).
    // временно отложим записи с пустым номером счета, т.к. он будет выступать в роли ключа для группировки.
    incomingDetails = Miscellanea
        .Filter(incomingDetails, acc => !string.IsNullOrWhiteSpace(acc.Number), out var emptyNumber);

    // затем среди пришедших оставили записи только тех счетов, у которых для номера счета однозначен id ЦКУ.
    comparers = new List<IEqualityComparer<CkuAccountDetail>>{ new AccountCkuIdComparer() };
    incomingDetails = Miscellanea
        .GetFactorizablyEquivalent(incomingDetails, acc => acc.Number, comparers, out var invalidCkuIdMissmatch);

    // вернем записи с пустым номером назад к набору анализируемых.
    incomingDetails = incomingDetails.Concat(emptyNumber).ToList();
    */
    IEnumerable<CkuAccountDetail> invalidCkuIdMissmatch = new List<CkuAccountDetail>();



    var result = new DocumentValidationResult()
    {
        ConsistentDetails = incomingDetails,
    };

    if (invalidNumberMissmatch.Any())
        result.FailedDetails[ValidationResult.fail_numberMissmatch] = invalidNumberMissmatch;

    if (invalidCkuIdMissmatch.Any())
        result.FailedDetails[ValidationResult.fail_CkuIdMissmatch] = invalidCkuIdMissmatch;

    var ERRORS = invalidCkuIdMissmatch.Select(det => det.CkuId).Distinct().ToArray();

    return result;    
}

private Dictionary<ValidationResult, IEnumerable<CkuAccountDetail>> ValidateInTable(IEnumerable<CkuAccountDetail> incomingDetails,
                                                                                    Dictionary<int, PersonalAccountMatchDTO> matchingTable)
{
    const int shakhtySettlementId = 21;    // id муниципального образования "город Шахты".

    // получаем ВСЕ шахтинские дома (не только подлежащие кап.ремонту), хранящиеся в базе.
    var shakhtyBuildingIds = _addressService.Formations
        .GetBuildingIds(shakhtySettlementId)  
        .ToArray();

    // для ВСЕХ шахтинских домов получаем имеющиеся в базе счета.
    var shakhtyAccountIds = _personalAccountService
        .GetPersonalAccountIdsByBuildingIdList(shakhtyBuildingIds)
        .SelectMany(pair => pair.Value)
        .ToArray();
    var shakhtyAccounts = _personalAccountService
        .GetPersonalAccountLargeCollection(shakhtyAccountIds)
        .ToList();

    // из шахтинских счетов выделяем те, для которых существует запись в таблице соответсвий id ЦКУ-ИБЖКХ.
    var ckuAccounts = shakhtyAccounts
        .Join(matchingTable, acc => acc.Id, match => match.Value.PersonalAccountId, (acc, match) =>
        new 
        {
            BillingId = acc.Id,
            CkuId     = match.Value.ExternId,
            Number    = acc.Number,
            IsClosed  = acc.EndDate.HasValue
        })
        .ToDictionary(acc => acc.CkuId);

    // среди пришедших оставляем только те счета с записью в таблице соответствий, которые реально хранятся в базе.
    incomingDetails = Miscellanea
        .Filter(incomingDetails, acc => ckuAccounts.ContainsKey(acc.CkuId), out var invalidInTableNotInDb);

    // среди пришедших оставляем только незакрытые счета.
    incomingDetails = Miscellanea
        .Filter(incomingDetails, acc => !ckuAccounts[acc.CkuId].IsClosed, out var invalidClosed);

    bool AreEqualNumbers(CkuAccountDetail acc)
    {
        string storedNumber = ckuAccounts[acc.CkuId].Number;

        return string.IsNullOrWhiteSpace(acc.Number) || acc.Number == storedNumber;    // пришедший номер счета может быть пустым, если его еще не сообщили ЦКУ.
    }

    // среди пришедших оставляем только счета с номерами, совпадающими с хранящимися у нас.
    incomingDetails = Miscellanea
        .Filter(incomingDetails, AreEqualNumbers, out var invalidMismatchedNumbers);

    // у счетов в базе и в таблице соответствий заполняем их id в системе биллинга.
    foreach (CkuAccountDetail acc in incomingDetails)
        acc.Stored = new CkuAccountAddInfo
        {
            PersonalAccountId = matchingTable[acc.CkuId].PersonalAccountId
        };

    var result = new Dictionary<ValidationResult,IEnumerable<CkuAccountDetail>>();

    if (incomingDetails.Any())
        result[ValidationResult.ok_FullyContained] = incomingDetails;

    if (invalidInTableNotInDb.Any())
        result[ValidationResult.fail_TableNotDbContained] = invalidInTableNotInDb;

    if (invalidClosed.Any())
        result[ValidationResult.fail_ClosedAccount] = invalidClosed;
        
    if (invalidMismatchedNumbers.Any())
        result[ValidationResult.fail_MissmatchedNumber] = invalidMismatchedNumbers;


    var ERROR = invalidMismatchedNumbers
        .Select(acc => $"cku= {acc.CkuId} income= {acc.Number} stored= {ckuAccounts[acc.CkuId].Number}")
        .Distinct()
        .ToArray();


    return result;
}

private Dictionary<ValidationResult, IEnumerable<CkuAccountDetail>> ValidateNotInTable(IEnumerable<CkuAccountDetail> incomingDetails,
                                                                                       Dictionary<int, PersonalAccountMatchDTO> matchingTable)
{
    // среди пришедших оставляем только счета с заполненными номерами. если номер пустой, то счет абсолютно новый.
    incomingDetails = Miscellanea
        .Filter(incomingDetails, acc => !string.IsNullOrWhiteSpace(acc.Number), out var validFresh);

    // по пришедшим номерам ищем соответствующие им счета в нашей базе.
    var numberSpec = new PersonalAccountNumberSpecification
    {
        Numbers = incomingDetails.Select(acc => acc.Number).ToArray()
    };
    var accountsByNumbers = _personalAccountService
        .GetPersonalAccountCollection(numberSpec, true)
        .ToDictionary(dto => dto.Number);

    // среди пришедших оставляем счета с известными нам номерами.
    incomingDetails = Miscellanea
        .Filter(incomingDetails, acc => accountsByNumbers.ContainsKey(acc.Number), out var invalidUnknownNumber);

    // среди пришедших оставляем не закрытые счета.
    incomingDetails = Miscellanea
        .Filter(incomingDetails, acc => !accountsByNumbers[acc.Number].EndDate.HasValue, out var invalidClosed);

    // извлекаем из таблицы соответствий все id ИБЖКХ счетов.
    var matchingTableBillingIds = new HashSet<int>(matchingTable.Select(pair => pair.Value.PersonalAccountId));

    // среди пришедших счетов НЕ из таблицы оставляем те, для которых рельно НЕ СУЩЕСТВУЕТ соответствующего ИБЖКХ id.
    incomingDetails = Miscellanea
        .Filter(incomingDetails, acc => !matchingTableBillingIds.Contains(accountsByNumbers[acc.Number].Id), out var invalidWrongNumber);

    // у счетов в базе, но не в таблице соответствий, заполняем их id в системе биллинга.
    foreach (CkuAccountDetail acc in incomingDetails)
        acc.Stored = new CkuAccountAddInfo
        {
            PersonalAccountId = accountsByNumbers[acc.Number].Id
        };

    var result = new Dictionary<ValidationResult,IEnumerable<CkuAccountDetail>>();

    if (incomingDetails.Any())
        result[ValidationResult.ok_DbContained] = incomingDetails;

    if (validFresh.Any())
        result[ValidationResult.ok_Fresh] = validFresh;

    if (invalidUnknownNumber.Any())
        result[ValidationResult.fail_UnknownNumber] = invalidUnknownNumber;

    if (invalidClosed.Any())
        result[ValidationResult.fail_ClosedAccount] = invalidClosed;

    if (invalidWrongNumber.Any())
        result[ValidationResult.fail_WrongNumber] = invalidWrongNumber;



    var ERROR = invalidUnknownNumber
        .Select(acc => $"cku= {acc.CkuId} income= {acc.Number}")
        .Distinct()
        .ToArray();



    return result;
}

// :::::::::::::::::::::::::::::::::::::::: Полезности. ::::::::::::::::::::::::::::::::::::::::

public static bool IsError(ValidationResult result)
{
    return result != ValidationResult.ok_FullyContained && 
           result != ValidationResult.ok_DbContained &&
           result != ValidationResult.ok_Fresh;
}

private class DocumentValidationResult
{
    public DocumentValidationResult()
    {
        ConsistentDetails = new List<CkuAccountDetail>();
        FailedDetails     = new Dictionary<ValidationResult, IEnumerable<CkuAccountDetail>>();
    }

    public IEnumerable<CkuAccountDetail> ConsistentDetails { get; set; }                              // записи документа, подлежащие дальнейшей обработке.
    public Dictionary<ValidationResult, IEnumerable<CkuAccountDetail>> FailedDetails { get; set; }    // записи документа с ошибками уже после начальной проверки.
}
}

}