using System;
using System.Collections.Generic;
using System.Linq;
using BillingServiceBus.PremiseServiceReference;
using IbZKH_CustomTypes.SingleTypes;
using IbZKH_CustomTypes.GenericTypes;
using Tools.Extensions;

namespace Import.API.Tools.ImportData.Importer.Tools.CkuCharge.Importers
{

public class CkuPremisesImporter
{
 
// :::::::::::::::::::::::::::::::::::::::: Содержимое. ::::::::::::::::::::::::::::::::::::::::

private const int CkuImportMethodType = (int)CommonService.Dictionaries.AddingPremiseTypeEnum.FileFromCKU;
private const int CkuOrganizationId   = 4733;

private readonly IPremiseService _premiseService;

// :::::::::::::::::::::::::::::::::::::::: Создание. ::::::::::::::::::::::::::::::::::::::::

public CkuPremisesImporter(IPremiseService premiseService)
{
    _premiseService = premiseService;
}

// :::::::::::::::::::::::::::::::::::::::: Работа. ::::::::::::::::::::::::::::::::::::::::

public IEnumerable<CkuPremisesImportDTO> ImportNewPremises(IEnumerable<CkuPremisesImportDTO> premisesToAdd_out)
{
    var errors = new List<string>();

    foreach (CkuPremisesImportDTO prem in premisesToAdd_out)
    {
        try    // если на каком-то этапе конвейера происходит ошибка, то переходим к следующему помещению, выбросив исключение.
        {
            AddNewPremisesIntoDb(prem);            // добавляем в базу новое помещение.
            AddNewOwnerIntoDb(prem.Owner);         // добавляем в базу нового владельца.
            BindPremisesOwner(prem);               // сязываем добавленное помещение и владельца.
            AddPremisesIntoMatchingTable(prem);    // добавляем помещение в таблицу соответствий.
        }
        catch (Exception ex)
        {
            errors.Add(ex.GetFullExceptionMessage());
        }
    }

    if (errors.Any())
        throw new Exception($"При импорте помещений произошла ошибка!{Environment.NewLine}" + string.Join(Environment.NewLine, errors));

    return premisesToAdd_out;    // возвращаем помещения и владельцев с заполненными идентификаторами.               
}

private CkuPremisesImportDTO AddNewPremisesIntoDb(CkuPremisesImportDTO prem_out)
{
    try
    {
        var dto = new PremiseAddDTO
        {
            BuildingId          = prem_out.BuildingId,
            Number              = prem_out.Number,
            TotalArea           = prem_out.TotalArea,
            PremiseTypeId       = prem_out.PremiseTypeId,
            AddingPremiseTypeId = CkuImportMethodType
        }; 

        OperationResult<Guid> wasAdded = _premiseService.AddPremiseInformation(dto, prem_out.BeginDate, prem_out.DocumentId);
        if (wasAdded.HasError)
            throw new Exception(wasAdded.GetErrors());
    
        // заполняем идентификатор помещения полученным значением.
        prem_out.Id = wasAdded.Result;    

        // возвращаем помещение с заполненным идентификатором.
        return prem_out;    
    }
    catch (Exception ex)
    {
        throw new Exception(ToMessage(prem_out, ex.GetFullExceptionMessage()));
    }
}

private CkuPremisesOwnerImportDTO AddNewOwnerIntoDb(CkuPremisesOwnerImportDTO owner_out)
{
    try
    {
        var dto = new OwnerPhysicalPersonDTO
        {
            Name       = owner_out.Name,
            Surname    = owner_out.Surname,
            Patronymic = owner_out.Patronymic
        };

        OperationResult<Guid> wasAdded = _premiseService.AddOwnerPhysicalPerson(dto);

        if (wasAdded.HasError)
            throw new Exception(wasAdded.GetErrors());

        owner_out.Id = wasAdded.Result;    // заполняем идентификатор владельца полученным значением.

        return owner_out;    // возвращаем владельца с заполненным идентификатором.
    }
    catch (Exception ex)
    {
        throw new Exception(ToMessage(owner_out, ex.GetFullExceptionMessage()));
    }
}

private void BindPremisesOwner(CkuPremisesImportDTO prem)
{
    try
    {
        var dto = new PremiseOwnerSetDTO
        {
            PhysicalPerson = new OwnerPhysicalPersonDTO
            {
                Id         = prem.Owner.Id.Value,    // СУЩЕСТВУЮЩЕМУ помещению добавляем СУЩЕСТВУЮЩЕГО владельца!
                Name       = prem.Owner.Name,
                Surname    = prem.Owner.Surname,
                Patronymic = prem.Owner.Patronymic
            },
            PropertyPartNumerator   = 1,
            PropertyPartDenominator = 1,
            BeginDate               = prem.BeginDate,
            BeginInputDate          = DateTime.Now
        };

        var newOwner  = new PremiseOwnerSetDTO[] { dto };
        var oldOwners = new int[] {};                       // мы не ставили помещению владельцев!

        OperationResult wasBinded = _premiseService.UpdatePremiseOwners(prem.Id.Value, newOwner, oldOwners, prem.DocumentId);

        if (wasBinded.HasError)
            throw new Exception(wasBinded.GetErrors());
    }
    catch (Exception ex)
    {
        throw new Exception(ToMessage(prem, ex.GetFullExceptionMessage()));
    }
}

private void AddPremisesIntoMatchingTable(CkuPremisesImportDTO prem)
{
    try
    {
        var dto = new PremiseMatchDTO
        {
            ZkhId = prem.Id.Value,
            ExternId = prem.CkuId,
            SupplierId = CkuOrganizationId
        };

        OperationResult wasAdded = _premiseService.AddPremiseMatch(dto);
        if (wasAdded.HasError)
            throw new Exception(wasAdded.GetErrors());
    }
    catch (Exception ex)
    {
        throw new Exception(ToMessage(prem, ex.GetFullExceptionMessage()));
    }
}

// :::::::::::::::::::::::::::::::::::::::: Полезности. ::::::::::::::::::::::::::::::::::::::::

string ToMessage(CkuPremisesImportDTO prem, string what)
{
    return $"Дом: {prem.BuildingId}, Помещение: {prem.Number}, Ошибка: {what}";   
}

string ToMessage(CkuPremisesOwnerImportDTO owner, string what)
{
    return $"Владелец: {owner.Surname} {owner.Name} {owner.Patronymic}, Ошибка: {what}";
}

}

}