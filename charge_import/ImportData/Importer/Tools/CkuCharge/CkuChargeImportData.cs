using System;
using System.Collections.Generic;
using Import.API.Model;
using Import.API.Tools.ImportData.Importer.Tools.CkuCharge.Validators;

namespace Import.API.Tools.ImportData.Importer.Tools.CkuCharge
{

public class CkuChargeImportData
{
    public string FileName { get; set; }
    public int PeriodId { get; set; }
    public DateTime DateOfImport { get; set; }
    public IEnumerable<CkuChargeDetail> Details { get;  set; } 
    public Dictionary<CkuPremisesValidator.ValidationResult, IEnumerable<CkuPremisesDetail>> PremisesDetails { get; set; }
    public Dictionary<CkuAccountValidator.ValidationResult, IEnumerable<CkuAccountDetail>> AccountsDetails { get; set; }
}

}