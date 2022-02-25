using System;

namespace Import.API.Tools.ImportData.Importer.Tools.CkuCharge.Importers
{

public class CkuPremisesOwnerImportDTO
{
    public Guid? Id { get; set; }
    public string Name { get; set; }
    public string Surname { get; set;}
    public string Patronymic { get; set; }
}

public class CkuPremisesImportDTO
{
    public Guid? Id { get; set; }
    public int CkuId { get; set; }
    public int BuildingId { get; set; }
    public string Number { get; set; }
    public decimal TotalArea { get; set; }
    public int PremiseTypeId { get; set; }
    public DateTime BeginDate {get; set; }
    public int DocumentId { get; set; }
    public CkuPremisesOwnerImportDTO Owner { get; set; }

}

}