namespace Import.API.Tools.ImportData.Importer.Tools.CkuCharge.Validators
{

public class CkuPremisesDetail
{
    public int Index { get; set; }    // индекс записи в документе. 
    public int CkuId { get; set; }
    public string UL_TP { get; set; }
    public string UL { get; set; }
    public string DOM { get; set; }
    public string KOR { get; set; }
    public string KV { get; set; }
    public string FiasId { get; set; }

    public CkuPremisesAddInfo Stored { get; set; }
}

}