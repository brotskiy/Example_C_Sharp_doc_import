using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Linq.Expressions;
using System.Security.Claims;
using BillingServiceBus.BillingServiceReference;
using BillingServiceBus.ChargeImportServiceReference;
using BillingServiceBus.DocumentServiceReference;
using BillingServiceBus.InternalInfrastructureServiceReference;
using BillingServiceBus.MeteringNodeServiceReference;
using BillingServiceBus.OverhaulServiceGateway;
using BillingServiceBus.PersonalAccountServiceReference;
using BillingServiceBus.PremiseServiceReference;
using BillingServiceBus.TokenGenerationService;
using IbZKH_CustomTypes.GenericTypes;
using IbZKH_CustomTypes.SingleTypes;
using IbZKH_CustomTypes.Specifications.Additionals;
using IbZKH_Extensions.Extensions;
using Ibzkh_SecurityAbstraction.Model;
using Import.API.Dto;
using Import.API.Infrastructure;
using Import.API.Model;
using Import.API.Tools.ImportData.Importer.Core;
using Import.API.Tools.ImportData.Importer.Tools.CkuCharge.Validators;
using Import.API.Tools.ImportData.Importer.Tools.CkuCharge.Importers;
using Import.API.Tools.ImportData.ImporterLog;
using Import.API.Tools.ImportData.Parcer;
using Import.API.Tools.ImportData.Importer.Tools.CkuCharge;
using Tools;
using Tools.Extensions;
using Bicycle.FExtensions;

namespace Import.API.Tools.ImportData.Importer
{
    public class CkuChargeImporter : ImporterBaseV2<CkuCharge, CkuChargeDetail, CkuChargeImportData>
    {
        private const int _ckuSupplierId = 4733;   // ЦКУ "Шахты"


        private readonly string _stubToken;


        #region CTOR

        public CkuChargeImporter(ImportLogBl importerLog, ImportServiceInfrastructure serviceInfrastructure)
            : base(importerLog, serviceInfrastructure) 
        {
            var claim = new Claim(CustomClaimTypes.SecutiryAccessRight, $"{SecurityAccessObjectEnum.PersonalData},{SecurityAccessActionEnum.RW}");
            _stubToken = new TokenGenerationServiceClient().GetBearerToken(new Claim[] { claim });
        }

        [Obsolete]
        public CkuChargeImporter() { }

        #endregion

        #region Overrides of ImporterBaseV2

        #region Parse and Save

        public override void ClearParcingDataFromModelImpl()
        {
            ServiceInfrastructure.Repository.Delete(DetailExpressionFilter, true);
            ServiceInfrastructure.Repository.Delete<CkuCharge>(x => x.ImportId == ImporterLog.Id, true);
        }

        public override CkuCharge ParceDataImpl(byte[] fileData)
        {
            var parsedData = CkuChargeParcer.Parce(fileData, ImporterLog.ModelWrapper.ModelInstance.FileName);  // заполняет дату периода из имени файла.
            TryDefinePeriodId(parsedData);                                                                      // определяет id периода на основе его заполненной даты.

            return parsedData;
        }

        public override OperationResult SaveDataToModelImpl(CkuCharge parcingObject)
        {
            var details = parcingObject.CkuChargeDetails;
            parcingObject.CkuChargeDetails = new List<CkuChargeDetail>();
            ServiceInfrastructure.Repository.Add(parcingObject);
            SaveRegisterHeader(parcingObject);
            ServiceInfrastructure.Repository.SaveChanges();

            details.ForEach(dtl => dtl.CkuChargeId = parcingObject.Id);
            ServiceInfrastructure.Repository.BulkInsert(details);

            return OperationResult.CreateSuccessResult($"Сохранено {details.Count} записей");
        }
        #endregion  // End of Parse and Save

        #region Prepare and Import

        public override CkuCharge GetParcingDataImpl()
        {
            CkuCharge res = ServiceInfrastructure.Repository.First<CkuCharge>(x => x.ImportId == ImporterLog.Id);
            var CkuChargeId = ServiceInfrastructure.Repository.QueryableSelect<CkuCharge>().First(x => x.ImportId == ImporterLog.Id).Id;
            res.CkuChargeDetails = ServiceInfrastructure.Repository.QueryableSelect<CkuChargeDetail>().AsNoTracking().Where(x => x.CkuChargeId == CkuChargeId).ToList();
            
            return res;
        }

        public override CkuChargeImportData PrepareDataForImportImpl(CkuCharge parcingData, RegisterHeader header)  // в начислениях ЦКУ хедер никак не используется.
        {
            return new CkuChargeImportData
            {
                FileName     = parcingData.FileName,
                PeriodId     = parcingData.PeriodId,
                DateOfImport = parcingData.PeriodDateFromFileName,
                Details      = parcingData.CkuChargeDetails.ToList()
            };
        }

        protected override ValidateResult ValidateBeforeImportImpl(CkuChargeImportData dataToBeImported_in_out)  
        {
            var serviceProvider = ServiceInfrastructure.ServiceProvider;

            var incomingPremisesDetails = dataToBeImported_in_out.Details
                .Select(detail => 
                new CkuPremisesDetail
                {
                    Index  = detail.Id,
                    CkuId  = detail.PREM_ID,
                    UL_TP  = detail.UL_TP,
                    UL     = detail.UL,
                    DOM    = detail.DOM,
                    KOR    = detail.KOR,
                    KV     = detail.KV,
                    FiasId = detail.FIASID
                })
                .ToList();
				
			var incomingAccountDetails = dataToBeImported_in_out.Details
                .Select(detail => 
                new CkuAccountDetail
                {
                    Index  = detail.Id,
                    CkuId  = detail.LS_ID,
                    Number = detail.LS
                })
                .ToList();		
				
			// проверка помещений и счетов независима! можно запустить параллельно!
            var validatedPremisesDetails = Task.Run(() => new CkuPremisesValidator(serviceProvider.GetService<IOverhaulServiceGateway>(_stubToken),
                                                                                   serviceProvider.GetService<IPremiseService>(_stubToken))
                                                              .Validate(incomingPremisesDetails));
            var validatedAccountDetails = Task.Run(() => new CkuAccountValidator(serviceProvider.GetService<IPersonalAccountService>(_stubToken))
                                                             .Validate(incomingAccountDetails));	
			Task.WaitAll()	
	
            dataToBeImported_in_out.PremisesDetails = validatedPremisesDetails.Result
                .Where(pair => !CkuPremisesValidator.IsError(pair.Key))
                .ToDictionary(pair => pair.Key, pair => pair.Value);

            dataToBeImported_in_out.AccountsDetails = validatedAccountDetails.Result
                .Where(pair => !CkuAccountValidator.IsError(pair.Key))
                .ToDictionary(pair => pair.Key, pair => pair.Value);

            bool withErroneousPremises = validatedPremisesDetails.Result.Keys
                .Any(key => CkuPremisesValidator.IsError(key));

            bool withErroneousAccounts = validatedAccountDetails.Result.Keys
                .Any(key => CkuAccountValidator.IsError(key));




            var goodPrems = dataToBeImported_in_out.PremisesDetails
                .SelectMany(prem => prem.Value)
                .Join(dataToBeImported_in_out.Details, prem => prem.Index, det => det.Id, (prem, det) => det)
                .ToList();

            var goodAccs = dataToBeImported_in_out.AccountsDetails
                .SelectMany(acc => acc.Value)
                .Join(dataToBeImported_in_out.Details, acc => acc.Index, det => det.Id, (acc, det) => det)
                .ToList();
            dataToBeImported_in_out.Details = goodPrems
                .Join(goodAccs, gprem => gprem.Id, gacc => gacc.Id, (gprem, gacc) => gprem)
                .ToList();
            /*
            if (withErroneousPremises || withErroneousAccounts)
                return ValidateResult.Failed("При проверке файла обнаружены ошибки!");    // YBR TODO вставить нормальные сообщения!        
            */




            return ValidateResult.Success();
        }

        protected override OperationResult<int> Import2BillingImpl(CkuChargeImportData dataToBeImported) 
        {
            int documentId = CreateDocument(dataToBeImported); 
            var premises   = ProcessPremises(dataToBeImported, documentId);
            ProcessAccounts(dataToBeImported, premises);
            ImportCkuCharges(dataToBeImported);

            return OperationResult<int>.CreateSuccessResult();
        }

        #endregion  // End of Prepare and Import.

        #region Load Detail

        protected override RegistryDetailInfo<CkuChargeDetail> LoadRegisterDetailImpl(PagerAdditionalSpecification paging)
        {
            // используется в Manual Editor Tools, которые для данного типа файла не предусмотрены.
            throw new NotImplementedException("CkuChargeImporter.LoadRegisterDetailImpl() не реализуется для импорта документов данного типа.");  
        }

        protected override List<PaymentRegistryManualDetailDto> Convert2ManualDetailImpl(List<CkuChargeDetail> parcingDetail)
        {
            // используется в Manual Editor Tools, которые для данного типа файла не предусмотрены.
            throw new NotImplementedException("CkuChargeImporter.Convert2ManualDetailImpl() не реализуется для импорта документов данного типа.");  
        }
        #endregion  // End of Load Detail.

        #region Supporting Tools

        protected override Expression<Func<CkuChargeDetail, bool>> DetailExpressionFilter => dtl => dtl.CkuCharge.ImportId == ImporterLog.Id;

        public override ImportDocSumStructure CalcTotalSum()
        {
            return new ImportDocSumStructure
            {
                RegisterSum = ServiceInfrastructure.Repository
                    .QueryableSelect(DetailExpressionFilter)
                    .IbDecimalSum(dtl => dtl.NACH)
            };
        }
        #endregion  // End of Supporting Tools.

        protected override RegisterHeaderDto GetDefaultRegisterHeaderImpl(CkuCharge parcingData)    // нужен только для совместимости с существующим механизмом импорта.
        {
            return new RegisterHeaderDto
            {
                NeedManualInput = false,
                BankAccountId = -1,                                               // костыль, чтобы выполнялась RegisterHeader.Validate().
                Date = parcingData.PeriodDateFromFileName,
                Number = $"charge{parcingData.PeriodDateFromFileName.Year}" +     // в номере будет имя файла.
                         $"{parcingData.PeriodDateFromFileName.Month}_sh",
                InformationProviderId = 4555,                                     // является ЦКУ.
                PeriodId = parcingData.PeriodId,
                ReceiverLegalEntityId = 1,
                ImportId = ImporterLog.Id
            };
        }
        #endregion  // End of Overrides of ImporterBaseV2.

        #region Parse and Save Logic

        private void TryDefinePeriodId(CkuCharge dataParsed)
        {
            if (dataParsed.PeriodDateFromFileName != DateTime.MinValue)
            {
                var period = ImporterResolver.GetPeriodByDate(dataParsed.PeriodDateFromFileName);

                if (period != null)
                    dataParsed.PeriodId = period.Id;
            }
        }

        private void SaveRegisterHeader(CkuCharge dataParsed)
        {
            var headerDto = GetDefaultRegisterHeaderImpl(dataParsed);
            var registerHeader = ServiceInfrastructure.Mapper.Map<RegisterHeader>(headerDto);

            bool headerExists = ServiceInfrastructure.Repository.Any<RegisterHeader>(rh => rh.ImportId == ImporterLog.Id);
            if (!headerExists)  // для данного ImportDataLog-a не существует заголовка реестра.
            {
                ServiceInfrastructure.Repository.Add(registerHeader);
            }
            else
            {
                ServiceInfrastructure.Repository.Update<RegisterHeader>(rh => rh.ImportId == registerHeader.ImportId,
                    rh => new RegisterHeader
                    {
                        BankAccountId = registerHeader.BankAccountId,
                        Date = registerHeader.Date,
                        Number = registerHeader.Number,
                        InformationProviderId = registerHeader.InformationProviderId,
                        PeriodId = registerHeader.PeriodId,
                        ReceiverLegalEntityId = registerHeader.ReceiverLegalEntityId,
                        ImportId = registerHeader.ImportId
                    });
            }
        }
        #endregion  // End of Parse and Save Logic.

        #region Prepare and Import Logic

        private int CreateDocument(CkuChargeImportData imported)
        {
            try
            {
                var documentService = ServiceInfrastructure.ServiceProvider.GetService<IDocumentService>(_stubToken);

                var docItem = new DocumentItem
                {
                    FileName    = imported.FileName,
                    Date        = imported.DateOfImport,
                    IsFileExist = false,
                    DigitalView = Array.Empty<byte>()
                };

                var document = documentService.SetDocument(docItem);
                if (document.HasError)
                    throw new Exception(document.GetErrors());

                return document.Result;
            }
            catch (Exception ex)
            {
                throw new Exception($"При создании документа произошла ошибка! {ex.GetFullExceptionMessage()}");
            }
        }

        #region Premises

        private IEnumerable<CkuPremisesImportDTO> ProcessPremises(CkuChargeImportData dataToBeImported,
                                                                  int documentId)
        {
            IEnumerable<CkuPremisesDetail> ExtractDistinctPremises(CkuPremisesValidator.ValidationResult validationKey)
            {
                return dataToBeImported.PremisesDetails
                    .Where(pair => pair.Key == validationKey)
                    .SelectMany(pair => pair.Value)
                    .GroupBy(prem => prem.CkuId, (key, group) => group.OrderBy(elem => elem.Index).First())
                    .ToList();
            }

            var incomingFresh    = ExtractDistinctPremises(CkuPremisesValidator.ValidationResult.ok_Fresh);
            var premisesImported = ImportNewPremises(dataToBeImported.Details,
                                                     incomingFresh,
                                                     dataToBeImported.DateOfImport,
                                                     documentId);

            var incomingStored  = ExtractDistinctPremises(CkuPremisesValidator.ValidationResult.ok_FullyContained);
            var premisesUpdated = UpdateStoredPremises(dataToBeImported.Details,
                                                       incomingStored, 
                                                       dataToBeImported.DateOfImport, 
                                                       documentId);

            return premisesImported.Concat(premisesUpdated).ToList(); 
        }

        private IEnumerable<CkuPremisesImportDTO> ImportNewPremises(IEnumerable<CkuChargeDetail> incomingDetails,
                                                                    IEnumerable<CkuPremisesDetail> incomingPremisesFresh,
                                                                    DateTime dateOfImport,
                                                                    int documentId)
        {
            var serviceProvider = ServiceInfrastructure.ServiceProvider;

            var premisesToImport = incomingPremisesFresh
                .Join(incomingDetails, prem => prem.Index, det => det.Id, (prem, det) => 
                new CkuPremisesImportDTO
                {
                    Id            = null,          // сейчас будет заполнено!
                    CkuId         = prem.CkuId,
                    BuildingId    = prem.Stored.BuildingId,
                    Number        = det.KV,
                    TotalArea     = det.S,
                    PremiseTypeId = det.IBZKH_TP_PREM,
                    BeginDate     = dateOfImport,
                    DocumentId    = documentId,
                    Owner         = new CkuPremisesOwnerImportDTO 
                    {
                        Id         = null,         // сейчас будет заполнено!
                        Name       = det.IM,
                        Surname    = det.FAM,
                        Patronymic = det.OT
                    }
                })
                .ToList();

            new CkuPremisesImporter(serviceProvider.GetService<IPremiseService>(_stubToken))
                .ImportNewPremises(premisesToImport);

            return premisesToImport;
        }

        private IEnumerable<CkuPremisesImportDTO> UpdateStoredPremises(IEnumerable<CkuChargeDetail> incomingDetails,
                                                                       IEnumerable<CkuPremisesDetail> incomingPremisesStored,
                                                                       DateTime dateOfImport,
                                                                       int documentId)
        {
            var serviceProvider = ServiceInfrastructure.ServiceProvider;

            var premisesService = serviceProvider.GetService<IPremiseService>(_stubToken);
            var iiService       = serviceProvider.GetService<IInternalInfrastructureService>(_stubToken);

            var premisesToUpdate = premisesService
                .GetPremiseInformationList(incomingPremisesStored.Select(prem => prem.Stored.PremisesId.Value).ToArray())
                .Join(incomingPremisesStored, stord => stord.Id, incm => incm.Stored.PremisesId.Value, (stord, incm) => new { stord, incm })
                .Join(incomingDetails, prem => prem.incm.Index, det => det.Id, (prem, det) =>
                (
                    Incoming  : prem.incm,
                    Stored    : prem.stord,
                    Detail    : det
                ))
                .ToList();

            if (premisesToUpdate.Count() != incomingPremisesStored.Count())
                throw new Exception("При обновлении помещений возникла ошибка: не удалось получить информацию обо всех требуемых помещениях!");
     
            var premisesOwners = premisesService
                .GetPremiseOwnerDTOsByPremisesIds(incomingPremisesStored.Select(prem => prem.Stored.PremisesId.Value).ToArray(), null)
                .GroupBy(owner => owner.PremiseId)
                .ToDictionary(group => group.Key, group => group.ToList());

            List<PremiseOwnerDTO> FindPremisesOwners(Guid premisesBillingId)
            {
                if (!premisesOwners.ContainsKey(premisesBillingId))
                    return new List<PremiseOwnerDTO>();
                else
                    return premisesOwners[premisesBillingId];
            }

            premisesToUpdate.ForEach(prem =>
            {
                if (NeedToUpdatePremisesArea(prem.Stored.TotalArea, prem.Detail.S))
                    UpdatePremisesArea(iiService, (prem.Stored, prem.Detail), dateOfImport, documentId);

                if (NeedToUpdatePremisesType(prem.Stored.PremiseType.Id, prem.Detail.IBZKH_TP_PREM))
                    UpdatePremisesType(premisesService, (prem.Stored, prem.Detail));

                var actualOwners = FindPremisesOwners(prem.Stored.Id);

                if (NeedToUpdatePremisesOwner(actualOwners, (prem.Stored, prem.Detail)))
                {
                    var newOwner = UpdatePremisesOwner(premisesService, 
                                                       (prem.Stored, prem.Detail), 
                                                       actualOwners.Select(owner => owner.Id).ToList(), 
                                                       dateOfImport, 
                                                       documentId);
                    premisesOwners[prem.Stored.Id] = new List<PremiseOwnerDTO>() { newOwner };
                }
            });

            var premisesUpdated = premisesToUpdate
                .Select(prem =>
                {
                    var owner = premisesOwners[prem.Stored.Id].Single().OwnerPhysicalPerson;
                    var dto   = new CkuPremisesImportDTO
                    {
                        Id            = prem.Stored.Id,
                        CkuId         = prem.Incoming.CkuId,
                        BuildingId    = prem.Stored.BuildingId,
                        Number        = prem.Incoming.KV,
                        TotalArea     = prem.Detail.S,
                        PremiseTypeId = prem.Detail.IBZKH_TP_PREM,
                        BeginDate     = dateOfImport,
                        DocumentId    = documentId,
                        Owner         = new CkuPremisesOwnerImportDTO 
                        {
                            Id         = owner.Id,       
                            Name       = owner.Name,
                            Surname    = owner.Surname,
                            Patronymic = owner.Patronymic
                        }
                    };

                    return dto;
                })
                .ToList();

            return premisesUpdated;
        }

        private bool NeedToUpdatePremisesArea(decimal? storedArea, decimal incomingArea)
        {
            const decimal EPSILON = 1e-5M;

            return !storedArea.HasValue || (Math.Abs(incomingArea - storedArea.Value) > EPSILON);
        }

        private void UpdatePremisesArea(IInternalInfrastructureService iiService,
                                        (PremiseDTO Stored, CkuChargeDetail Incoming) prem,
                                        DateTime dateOfImport,
                                        int documentId)
        {
            try
            {
                const int PREMISE_AREA_PARAMETER_TYPE = 195;    // идентификатор рассчетного параметра типа "Площадь".

                var linkToPremise = new BillingServiceBus.InternalInfrastructureServiceReference.LinqToObjectDTO
                {
                    ObjectType = BillingServiceBus.InternalInfrastructureServiceReference.LinqObjectEnum.PremiseInformation,
                    ObjectId   = new BillingServiceBus.InternalInfrastructureServiceReference.GuidValueDTO 
                    { 
                        Value =  prem.Stored.Id 
                    }
                };

                var areaParameter = new BillingServiceBus.InternalInfrastructureServiceReference.CalculatingParameterBaseDTO[1]
                {
                    new BillingServiceBus.InternalInfrastructureServiceReference.CalculatingParameterBaseDTO
                    {
                        CalculatingParameterType = new KeyValueItem<int>(PREMISE_AREA_PARAMETER_TYPE, "TotalArea"),
                        DecimalValue = prem.Incoming.S
                    }
                };

                var wasUpdated = iiService.AddParameterDetails(linkToPremise, areaParameter, dateOfImport, documentId);
                if (wasUpdated.HasError)
                    throw new Exception(wasUpdated.GetErrors());
            }
            catch (Exception ex)
            {
                throw new Exception($"При обновлении площади помещения с id={prem.Stored.Id} произошла ошибка: {ex.GetFullExceptionMessage()}");
            }
        }

        private bool NeedToUpdatePremisesType(int storedType, int incomingType)
        {
            return storedType != incomingType;
        }

        private void UpdatePremisesType(IPremiseService premisesService,
                                        (PremiseDTO Stored, CkuChargeDetail Incoming) prem)
        {
            try        
            {
                var dto = new PremiseUpdateDTO
                {
                    Id              = prem.Stored.Id,
                    Number          = prem.Stored.Number,
                    HabitableArea   = prem.Stored.HabitableArea,
                    PremiseTypeId   = prem.Incoming.IBZKH_TP_PREM,
                    CadastralNumber = prem.Stored.CadastralNumber,
                    FloorNumber     = prem.Stored.FloorNumber,
                    PorchNumber     = prem.Stored.PorchNumber
                };

                var wasUpdated = premisesService.UpdatePremiseInformation(dto);
                if (wasUpdated.HasError)
                    throw new Exception(wasUpdated.GetErrors());
            }
            catch (Exception ex)
            {
                throw new Exception($"При обновлении типа помещения с id={prem.Stored.Id} произошла ошибка: {ex.GetFullExceptionMessage()}");
            }
        }

        private bool NeedToUpdatePremisesOwner(IEnumerable<PremiseOwnerDTO> premisesOwners, 
                                               (PremiseDTO Stored, CkuChargeDetail Incoming) prem)
        {
            bool isPhysical(PremiseOwnerDTO owner)
            {
                return owner.OwnerPhysicalPerson != null;
            }

            bool areSameNames(BillingServiceBus.PremiseServiceReference.OwnerPhysicalPersonDTO old, 
                              CkuChargeDetail incoming)
            {
                return old.Name == incoming.IM
                    && old.Surname == incoming.FAM 
                    && old.Patronymic == incoming.OT;
            }

            // если собственников вообще нет, то их нужно обновлять.
            if (!premisesOwners.Any())            
                return true;           

            bool withPhysicals = premisesOwners.Any(owner => isPhysical(owner));
            bool withLegals    = premisesOwners.Any(owner => owner.OwnerLegalEntityId != null);

            if (withPhysicals && withLegals)
                throw new Exception($"Для помещения id={prem.Stored.Id} собственниками одновременно установлены физ. и юр. лица!");

            // ЦКУ-шное помещение имеет битых хозяев, если им владеют юр.лица либо НЕСКОЛЬКО физ.лиц. в этом случае ставим пришедшего собственника.
            bool withErrors = withLegals || (premisesOwners.Count(owner => isPhysical(owner)) > 1);
            if (withErrors)
                return true;

            // если текущий собственник - единственное физ.лицо, то сравним имена.
            bool areEqualOwners = areSameNames(premisesOwners.Single(owner => isPhysical(owner)).OwnerPhysicalPerson, prem.Incoming); 
            
            // собственника нужно обновить, если у него другое имя.
            return !areEqualOwners;
        }
        private PremiseOwnerDTO UpdatePremisesOwner(IPremiseService premisesService,
                                                    (PremiseDTO Stored, CkuChargeDetail Incoming) prem,
                                                    IEnumerable<int> ownersToDelete,
                                                    DateTime dateOfImport,
                                                    int documentId)
        {
            try
            {
                var ownerToCreate = new PremiseOwnerSetDTO[1]
                {
                    new PremiseOwnerSetDTO
                    {
                        BeginDate               = dateOfImport,
                        BeginInputDate          = DateTime.Now,
                        PropertyPartNumerator   = 1,
                        PropertyPartDenominator = 1,
                        PhysicalPerson          = new BillingServiceBus.PremiseServiceReference.OwnerPhysicalPersonDTO
                        {
                            Name       = prem.Incoming.IM,
                            Surname    = prem.Incoming.FAM,
                            Patronymic = prem.Incoming.OT
                        }
                    }
                };

                var wasUpdated = premisesService.UpdatePremiseOwners(prem.Stored.Id, ownerToCreate, ownersToDelete.ToArray(), documentId);
                if (wasUpdated.HasError)
                    throw new Exception(wasUpdated.GetErrors());

                var newOwner = premisesService
                    .GetPremiseOwnerDTOsByPremisesIds(new Guid[1]{ prem.Stored.Id }, null)
                    .Single();

                return newOwner;
            }
            catch (Exception ex)
            {
                throw new Exception($"При обновлении владельца помещения с id={prem.Stored.Id} произошла ошибка: {ex.GetFullExceptionMessage()}");    
            }
        }

        #endregion    // End of Premises.

        #region Personal Accounts

        private void ProcessAccounts(CkuChargeImportData dataToBeImported,
                                     IEnumerable<CkuPremisesImportDTO> premisesProcessed)
        {
            IEnumerable<CkuAccountDetail> ExtractDistinctAccounts(CkuAccountValidator.ValidationResult validationKey)
            {
                return dataToBeImported.AccountsDetails
                    .Where(pair => pair.Key == validationKey)
                    .SelectMany(pair => pair.Value)
                    .GroupBy(acc => acc.CkuId, (key, group) => group.OrderBy(elem => elem.Index).First())
                    .ToList();    
            }

            var incomingFresh    = ExtractDistinctAccounts(CkuAccountValidator.ValidationResult.ok_Fresh);
            var accountsImported = ImportNewAccounts(dataToBeImported.Details, 
                                                     incomingFresh, 
                                                     premisesProcessed,
                                                     dataToBeImported.DateOfImport,
                                                     dataToBeImported.PeriodId);

            var incomingStored = ExtractDistinctAccounts(CkuAccountValidator.ValidationResult.ok_DbContained)
                .Concat(ExtractDistinctAccounts(CkuAccountValidator.ValidationResult.ok_FullyContained))
                .ToList();
            var accountsUpdated = UpdateStoredAccounts(dataToBeImported.Details, 
                                                       incomingStored, 
                                                       premisesProcessed, 
                                                       dataToBeImported.DateOfImport,
                                                       dataToBeImported.PeriodId);

            var accountsToBind = accountsUpdated
                .Where(acc => acc.MeteringNode.Id == null)
                .ToList();

            var serviceProvider = ServiceInfrastructure.ServiceProvider;
            new CkuAccountImporter(serviceProvider.GetService<IBillingService>(_stubToken),
                                   serviceProvider.GetService<IPersonalAccountService>(_stubToken))
                .BindAccountsAndPremises(accountsToBind);
        }

        private IEnumerable<CkuAccountImportDTO> ImportNewAccounts(IEnumerable<CkuChargeDetail> incomingDetails,
                                                                   IEnumerable<CkuAccountDetail> incomingAccountsFresh,
                                                                   IEnumerable<CkuPremisesImportDTO> premisesProcessed,
                                                                   DateTime dateOfImport,
                                                                   int periodId)
        {
            var serviceProvider = ServiceInfrastructure.ServiceProvider;

            var premisesInfo = premisesProcessed
                .ToDictionary(prem => prem.CkuId);

            var accountsToImport = incomingAccountsFresh
                .Join(incomingDetails, acc => acc.Index, det => det.Id, (acc, det) =>
                new CkuAccountImportDTO
                {
                    Id = null,    // сейчас будет заполнено!
                    CkuId = acc.CkuId,
                    Number = acc.Number,
                    BeginDate = new DateTime(2014, 5, 1),    // дата запуска фонда!
                    PeriodId = periodId,
                    PayerPhysicalPersonId = premisesInfo[det.PREM_ID].Owner.Id.Value,
                    Premises = premisesInfo[det.PREM_ID],      
                    MeteringNode = new CkuMeteringNodeImportDTO
                    {
                        Id = null,    // сейчас будет заполнено!
                        OverhaulTariff = det.TARIFF,
                        BeginDate = dateOfImport,
                        BeginInputDate = dateOfImport
                    }
                })
                .ToList();

            new CkuAccountImporter(serviceProvider.GetService<IBillingService>(_stubToken),
                                   serviceProvider.GetService<IPersonalAccountService>(_stubToken))
                .ImportNewAccounts(accountsToImport);

            return accountsToImport;
        }

        private IEnumerable<CkuAccountImportDTO> UpdateStoredAccounts(IEnumerable<CkuChargeDetail> incomingDetails,
                                                                      IEnumerable<CkuAccountDetail> incomingAccoutsStored,
                                                                      IEnumerable<CkuPremisesImportDTO> premisesProcessed,
                                                                      DateTime dateOfImport,
                                                                      int periodId)
        {
            var serviceProvider     = ServiceInfrastructure.ServiceProvider;
            var accountService      = serviceProvider.GetService<IPersonalAccountService>(_stubToken);
            var meteringNodeService = serviceProvider.GetService<IMeteringNodeService>(_stubToken);
            var ckuAccountImporter  = new CkuAccountImporter(serviceProvider.GetService<IBillingService>(_stubToken), accountService);

            var ACCOUNT_MATCHES = accountService
                .GetPersonalAccountMatches(_ckuSupplierId)
                .ToDictionary(match => match.ExternId);

            var premisesInfo = premisesProcessed
                .ToDictionary(prem => prem.CkuId);

            var accountsToUpdate = accountService
                .GetPersonalAccountLargeCollection(incomingAccoutsStored.Select(acc => acc.Stored.PersonalAccountId).ToArray())
                .Join(incomingAccoutsStored, stord => stord.Id, incm => incm.Stored.PersonalAccountId, (stord, incm) => new { stord, incm })
                .Join(incomingDetails, acc => acc.incm.Index, det => det.Id, (acc, det) => 
                (
                    Incoming      : acc.incm,
                    Stored        : acc.stord,
                    Detail        : det,
                    Prem          : premisesInfo[det.PREM_ID]
                ))
                .ToList();
            
            if (accountsToUpdate.Count() != incomingAccoutsStored.Count())
                throw new Exception("При обновлении счетов возникла ошибка: не удалось получить информацию обо всех требуемых счетах!");

            bool IsInMatchingTable(int accountCkuId)
            {
                return ACCOUNT_MATCHES.ContainsKey(accountCkuId);
            };

            accountsToUpdate.ForEach(acc => 
            {
                try
                {
                    if (!IsInMatchingTable(acc.Incoming.CkuId))
                        ckuAccountImporter.AddAccountIntoMatchingTable(acc.Incoming.CkuId, acc.Stored.Id);

                    if (NeedToUpdateAccountPayer(acc.Stored.Payer, acc.Prem.Owner))
                        UpdateAccountPayer(accountService, acc.Stored.Id, acc.Prem.Owner.Id.Value);
                }
                catch (Exception ex)
                {
                    throw new Exception($"При обновлении счета с id={acc.Stored.Id} произошла ошибка: {ex.GetFullExceptionMessage()}");
                }
            });

            bool IsValidNode(BillingServiceBus.MeteringNodeServiceReference.MeteringNodeDTO node)
            {
                return node.EndDate == null
                    && node.PremiseInformationId != null
                    && node.UtilityService.Id == (int)UtilityServiceEnum.Overhaul;
            }

            var meteringNodes = meteringNodeService
                .GetMeteringNodesCollectionByPersonalAccountCollection(incomingAccoutsStored.Select(acc => acc.Stored.PersonalAccountId).ToArray())
                .GroupBy(node => node.PersonalAccount.Id)
                .ToDictionary(group => group.Key, group => group.Where(node => IsValidNode(node)).ToList());

            var accountsUpdated = accountsToUpdate
                .Select(acc => 
                {
                    var meteringNode = meteringNodes.ContainsKey(acc.Stored.Id)
                        ? meteringNodes[acc.Stored.Id].Where(node => node.PremiseInformationId == acc.Prem.Id.Value).FirstOrDefault()
                        : null;

                    return new CkuAccountImportDTO
                    {
                        Id = acc.Stored.Id,
                        CkuId = acc.Incoming.CkuId,
                        Number = acc.Stored.Number,
                        BeginDate = new DateTime(2014, 5, 1),    // дата запуска фонда!
                        PeriodId = periodId,
                        PayerPhysicalPersonId = acc.Prem.Owner.Id.Value,
                        Premises = acc.Prem,      
                        MeteringNode = new CkuMeteringNodeImportDTO
                        {
                            Id             = meteringNode?.Id,    // при наличии узла учета между счетом и помещением записываем его id!
                            OverhaulTariff = acc.Detail.TARIFF,
                            BeginDate      = dateOfImport,
                            BeginInputDate = dateOfImport
                        }
                    };
                })
                .ToList();
    
            return accountsUpdated;        
        }

        private bool NeedToUpdateAccountPayer(PersonalAccountPayerDTO stored, CkuPremisesOwnerImportDTO incoming)
        {
            bool withLegal    = stored.LegalEntityId != null;
            bool withPhysical = stored.PhysicalPerson != null;

            if (withLegal && withPhysical)
                throw new Exception("У лицевого счета не могут быть одновременно установлены плательщики юр. и физ.лицо!");

            if (withPhysical)
                return stored.PhysicalPerson.Id != incoming.Id.Value;    // если текущий плательщик - физ.лицо, то сравним идентификаторы.

            return true;    // во всех остальных случаях на месте плательщика какой-то мусор, и нужно его обновить.
        }

        private void UpdateAccountPayer(IPersonalAccountService accountService,
                                        int accountBillingId,
                                        Guid newPhysicalPayerId)
        {
            var dto = new PersonalAccountPayerSetDTO
            {
                PhysicalPersonId = newPhysicalPayerId
            };
            
            var wasUpdated = accountService.AddPersonalAccountPayer(accountBillingId, dto);
            if (wasUpdated.HasError)
                throw new Exception(wasUpdated.GetErrors());
        }

        #endregion    // End of Personal Accounts.

        private void ImportCkuCharges(CkuChargeImportData dataToBeImported)
        {
            try
            {
                const int OVERHAUL_FUND_SUPPLIER_ID = 1;

                var serviceProvider = ServiceInfrastructure.ServiceProvider;
                
                var PREMISES_MATCHES = serviceProvider.GetService<IPremiseService>(_stubToken)
                    .GetPremiseMatches(_ckuSupplierId)
                    .ToDictionary(match => match.ExternId);

                var ACCOUNT_MATCHES = serviceProvider.GetService<IPersonalAccountService>(_stubToken)
                    .GetPersonalAccountMatches(_ckuSupplierId)
                    .ToDictionary(match => match.ExternId);

                var incomingAccountBillingIds = dataToBeImported.Details
                    .Select(det => ACCOUNT_MATCHES[det.LS_ID].PersonalAccountId)
                    .Distinct()
                    .ToArray();

                var incomingPremisesBillingIds = new HashSet<Guid>(dataToBeImported.Details
                    .Select(det => PREMISES_MATCHES[det.PREM_ID].ZkhId)
                    .Distinct());

                bool IsOfInterest(MeteringNodeFullDTO node)
                {
                    return node.PremiseInformationId.HasValue
                        && incomingPremisesBillingIds.Contains(node.PremiseInformationId.Value)
                        && node.EndDate           == null
                        && node.UtilityService.Id == (int)UtilityServiceEnum.Overhaul
                        && node.SupplierId        == OVERHAUL_FUND_SUPPLIER_ID;
                }

                var meteringNodes = serviceProvider.GetService<IMeteringNodeService>(_stubToken)
                    .GetFullMeteringNodesCollectionByPersonalAccountCollection(incomingAccountBillingIds)
                    .GroupBy(node => node.PersonalAccount.Id, (key, group) => 
                        group
                        .Where(node => IsOfInterest(node))
                        .GroupBy(node => node.PremiseInformationId.Value, (key2, group2) => group2.First()))
                    .SelectMany(grouped => grouped)
                    .ToDictionary(node => (node.PersonalAccount.Id, node.PremiseInformationId.Value));

                var chargesToImport = dataToBeImported.Details
                    .Select(det => 
                    {
                        int accId   = ACCOUNT_MATCHES[det.LS_ID].PersonalAccountId;
                        Guid premId = PREMISES_MATCHES[det.PREM_ID].ZkhId;
                        var node    = meteringNodes[(accId, premId)];

                        var recalculationPeriod = string.IsNullOrWhiteSpace(det.REC_PER)
                            ? null
                            : new int?(ImporterResolver.GetPeriodByCode(det.REC_PER).Id);

                        return new ChargeImportDto
                        {
                            CostValue = det.TARIFF,
                            Volume    = det.CNT,
                            Sum       = det.NACH,
                            Date      = dataToBeImported.DateOfImport,
                            PeriodId  = dataToBeImported.PeriodId,
                            RecalculationPeriodId = recalculationPeriod,
                            SupplierId = node.SupplierId,
                            PersonalAccountId = accId,
                            MeteringNodeId = node.Id,
                            UtilityServiceId = node.UtilityService.Id,
                            TariffId = node.UtilityTariff.Id,
                            ConsumptionTypeId = ConsumptionTypeEnum.Norm,
                            ChargeTypeId = det.IBZKH_CHARGE_TYPE_ID
                        };
                    })
                    .ToArray();

                var wereAdded = serviceProvider.GetService<IChargeImportService>(_stubToken).AddCkuCharges(chargesToImport);
                if (wereAdded.HasError)
                    throw new Exception(wereAdded.GetErrors());
            }
            catch (Exception ex)
            {
                throw new Exception($"При импорте начислений произошла ошибка: {ex.GetFullExceptionMessage()}");
            }
        }

        #endregion  // End of Prepare and Import Logic.
    } 
}
