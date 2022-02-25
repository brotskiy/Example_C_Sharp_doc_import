namespace Import.API.Tools.ImportData.Importer.Tools.CkuCharge.Validators
{

public class CkuAccountDetail
{
    public int Index { get; set; }    // индекс записи в документе. 
    public int CkuId { get; set; }
    public string Number { get; set; }

    public CkuAccountAddInfo Stored { get; set; }
}

}