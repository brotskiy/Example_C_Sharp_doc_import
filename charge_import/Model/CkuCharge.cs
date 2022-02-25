using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Serialization;
using Import.API.Tools.ImportData.Importer.Core;

namespace Import.API.Model
{
    public class CkuCharge : IImportDocument<CkuCharge,CkuChargeDetail>  
    {
        public int Id { get; set; }

        public int PeriodId { get; set; }

        public string FileName { get; set; }

        public DateTime PeriodDateFromFileName { get; set; }

        [XmlIgnore] 
        public ICollection<CkuChargeDetail> CkuChargeDetails { get; set; } = new List<CkuChargeDetail>();

        #region Navigation
        public int ImportId { get; set; }
        public ImportDataLog Import { get; set; }
        #endregion  // End of Navigation.

        #region IImportDocument

        public List<CkuChargeDetail> GetDetails()
        {
            return CkuChargeDetails.ToList();
        }

        public ValidateResult ParcingValidate()
        {
            ValidateResult result = ValidateResult.Success();

            if (!CkuChargeDetails.Any())
            {
                result.AddMessage("Нет детализации.");
                return result;
            }

            if (PeriodDateFromFileName == DateTime.MinValue)
                result.AddMessage($"Имя файла \"{FileName}\" имеет неверный формат: ошибка в дате расчетного периода.");

            var periodsCount = CkuChargeDetails.Select(detail => detail.PER).Distinct().Count();
            if (periodsCount > 1)
            {
                result.AddMessage("Файл содержит записи из разных расчетных периодов.");
                return result;
            }

            foreach (CkuChargeDetail detail in CkuChargeDetails)
            {
                ValidateResult detailRes = detail.ParcingValidate(PeriodDateFromFileName);

                if (detailRes.HasError)
                    result.AddMessage(detailRes.Message);
            }

            return result;
        }
        #endregion  // End of IImportDocument.
    }
}