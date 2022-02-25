using System;

namespace Import.API.Tools.ImportData.Importer.Tools.CkuCharge.Importers
{

public class CkuMeteringNodeImportDTO
{
    public int? Id { get; set; }
    public decimal OverhaulTariff { get; set; }    // величина тарифа кап.ремонта.
    public DateTime BeginDate { get; set; }
    public DateTime BeginInputDate { get; set; }
}

public class CkuAccountImportDTO
{
    public int? Id { get; set; }
    public int CkuId { get; set; }
    public string Number { get; set; }
    public DateTime BeginDate { get; set; }
    public int PeriodId { get; set; }
    public Guid PayerPhysicalPersonId { get; set; }
    public CkuPremisesImportDTO Premises { get; set; }            // помещение, с которым связан данный лицевой счет.
    public CkuMeteringNodeImportDTO MeteringNode { get; set; }    // узел учета, посредством которого счет связан с помещением.
}

}