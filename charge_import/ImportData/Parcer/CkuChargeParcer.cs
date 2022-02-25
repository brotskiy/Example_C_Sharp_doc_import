using System;
using System.Data;
using System.Globalization;
using IbZKH_Extensions.ExtendedTools.Dbf;
using Import.API.Model;
using System.Text.RegularExpressions;

namespace Import.API.Tools.ImportData.Parcer
{
    public static class CkuChargeParcer
    {
        public static CkuCharge Parce(byte[] fileData, string fileName)  // распаковка архива сделана на этапе ImportRunner, здесь уже имеем содержимое файла.
        {
            CkuCharge result = CreateFromFileName(fileName);

            StreamDbfReader dbfReader = new StreamDbfReader();
            DataTable dataTable = dbfReader.GetDbfTable(fileData);

            foreach (DataRow row in dataTable.Rows)
            {
                DataRowReader rowReader = new DataRowReader(row);

                CkuChargeDetail newItem = new CkuChargeDetail
                {
                    PAY_ID = rowReader.ReadField<int>("ID"),         // должно быть всегда заполнено. (отсутствие этой проверки не критично.)
                    LS_ID = rowReader.ReadField<int>(nameof(CkuChargeDetail.LS_ID)),           // должно быть всегда заполнено.
                    LS_CKU = rowReader.ReadField<string>(nameof(CkuChargeDetail.LS_CKU)),
                    LS = rowReader.ReadField<string>(nameof(CkuChargeDetail.LS)),
                    PREM_ID = rowReader.ReadField<int>(nameof(CkuChargeDetail.PREM_ID)),       // должно быть всегда заполнено.
                    PER = rowReader.ReadField<string>(nameof(CkuChargeDetail.PER)),            // должно быть всегда заполнено.
                    UL_TP = rowReader.ReadField<string>(nameof(CkuChargeDetail.UL_TP)),
                    UL = rowReader.ReadField<string>(nameof(CkuChargeDetail.UL)),
                    DOM = rowReader.ReadField<string>(nameof(CkuChargeDetail.DOM)),
                    KOR = rowReader.ReadField<string>(nameof(CkuChargeDetail.KOR)),
                    KV = rowReader.ReadField<string>(nameof(CkuChargeDetail.KV)),
                    S = rowReader.ReadField<decimal>(nameof(CkuChargeDetail.S)),        
                    
                    TP_NACH = rowReader.ReadField<int>(nameof(CkuChargeDetail.TP_NACH)),
                    TARIFF = rowReader.ReadField<decimal>(nameof(CkuChargeDetail.TARIFF)),
                    CNT = rowReader.ReadField<decimal>(nameof(CkuChargeDetail.CNT)),
                    NACH = rowReader.ReadField<decimal>(nameof(CkuChargeDetail.NACH)),
                    REC_PER = rowReader.ReadField<string>(nameof(CkuChargeDetail.REC_PER)),
                    RS_CH = rowReader.ReadField<string>(nameof(CkuChargeDetail.RS_CH)),
                    FAM = rowReader.ReadField<string>(nameof(CkuChargeDetail.FAM)),
                    IM = rowReader.ReadField<string>(nameof(CkuChargeDetail.IM)),
                    OT = rowReader.ReadField<string>(nameof(CkuChargeDetail.OT)),
                    LS_GIS = rowReader.ReadField<string>(nameof(CkuChargeDetail.LS_GIS)),
                    FIASID = rowReader.ReadField<string>(nameof(CkuChargeDetail.FIASID)),

                    IBZKH_CHARGE_TYPE_ID = CkuChargeDetail.DefineIbzkhChargeTypeId(rowReader.ReadField<int>("ID_USL")),
                    IBZKH_TP_PREM = CkuChargeDetail.DefineIbzkhPremiseTypeId(rowReader.ReadField<int>("TP_PREM")), 

                    CkuCharge = result,               
                };

                result.CkuChargeDetails.Add(newItem);
            }

            return result;
        }
        
        private static CkuCharge CreateFromFileName(string fileName)
        {
            CkuCharge result = new CkuCharge 
            { 
                PeriodDateFromFileName = DateTime.MinValue,
                FileName = fileName
            };

            if (!string.IsNullOrEmpty(fileName))
            {
                string pattern = @"CHARGE\d{6}";

                if ((new Regex(pattern, RegexOptions.IgnoreCase)).Matches(fileName).Count == 1)    // если в имени файла найдено СТРОГО одно соответствие.
                {
                    string dateString = new Regex(@"\d{6}").Match(fileName).Value + "01";

                    if (DateTime.TryParseExact(dateString, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateValue))
                        result.PeriodDateFromFileName = dateValue;
                }
            }        

            return result;
        }
    }
}