using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using BaseEntities.Interfaces;
using CommonService.Dictionaries;
using Import.API.Tools.ImportData.Importer.Core;
using Tools.Validation;

namespace Import.API.Model
{
    public class CkuChargeDetail : IIdentityEntity<int>
    {
        public int Id { get; set; }
        public int PAY_ID { get; set; }
        public int LS_ID { get; set; }
        public string LS_CKU { get; set; }
        public string LS { get; set; }
        public int PREM_ID { get; set; }
        public string PER { get; set; }
        public string UL_TP { get; set; }
        public string UL { get; set; }
        public string DOM { get; set; }
        public string KOR { get; set; }
        public string KV { get; set; }
        public decimal S { get; set; }              
        public int TP_NACH { get; set; }
        public decimal TARIFF { get; set; }   
        public decimal CNT { get; set; }       
        public decimal NACH { get; set; }     
        public string REC_PER { get; set; }     
        public string RS_CH { get; set; }
        public string FAM { get; set; }
        public string IM { get; set; }
        public string OT { get; set; }
        public string LS_GIS { get; set; }
        public string FIASID { get; set; }

        public int IBZKH_CHARGE_TYPE_ID { get; set; }  // тип начисления: оплата/пеня/и т.д.
        public int IBZKH_TP_PREM { get; set; }        // тип помещения в системе ИБЖКХ.

        #region Navigation
        public int CkuChargeId { get; set; }
        public CkuCharge CkuCharge { get; set; }
        #endregion

        private static class ActualCkuPremiseIds  // допустимые значения, которые может принимать поле c типом помещения TP_PREM.
        {
            public static HashSet<int> Habitable = new HashSet<int> { 3, 8 };   // шахтинские типы TP_PREM, относящиеся к ЖИЛЫМ помещениям.
            public static HashSet<int> Uninhabited = new HashSet<int> { 9 };    // шахтинские типы TP_PREM, относящиеся к НЕЖИЛЫМ помещениям.
        }

        public ValidateResult ParcingValidate(DateTime PeriodDateFromFileName)
        {
            ValidateResult result = ValidateResult.Success();

            if (LS_ID == default)
                result.AddMessage($"Начисление {PAY_ID}: поле с ЦКУ id лицевого счета не заполнено.");

            //if (!string.IsNullOrWhiteSpace(LS) && !Verhoeff.ValidateVerhoeff(LS))
            //    result.AddMessage($"Начисление {PAY_ID}: номер лицевого счета {LS} не прошел валидацию.");

            if (PREM_ID == default)
                result.AddMessage($"Начисление {PAY_ID}: поле с ЦКУ id помещения не заполнено.");

            if (IBZKH_TP_PREM == -1)   // если после трансляции значение ибжкх-шного типа помещения стало null.
            {
                var hab = ActualCkuPremiseIds.Habitable.ToList();
                var uninhab = ActualCkuPremiseIds.Uninhabited.ToList();

                result.AddMessage($"Начисление {PAY_ID}: значение поля \"TP_PREM\", содержащего тип помещения в системе ЦКУ, является недопустимым. " +
                                  $"Разрешены значения: {string.Join(",", hab.Union(uninhab))}");
            }

            if (string.IsNullOrWhiteSpace(FIASID))
            {
                result.AddMessage($"Начисление {PAY_ID}: поле с id ФИАС не заполнено.");
            }
            else
            {
                if (!Guid.TryParse(FIASID, out var fiasId))
                    result.AddMessage($"Начисление {PAY_ID}: id ФИАС не может быть распознан.");
            }

            if (string.IsNullOrWhiteSpace(PER))
            {
                result.AddMessage($"Начисление {PAY_ID}: поле с расчетным периодом не заполнено.");
            }
            else
            {
                bool isParsed = DateTime.TryParseExact($"01{PER}", "ddMMyyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out var periodDate);
                if (!isParsed)
                {
                    result.AddMessage($"Начисление {PAY_ID}: расчетный период \"{PER}\" имеет недопустимый формат.");
                }
                else
                {
                    if (periodDate > DateTime.Now)
                        result.AddMessage($"Начисление {PAY_ID}: дата расчетного периода \"{periodDate.Year}.{periodDate.Month}.{periodDate.Day}\" больше текущей даты.");

                    if (PeriodDateFromFileName != DateTime.MinValue && 
                        PeriodDateFromFileName != periodDate)
                        result.AddMessage($"Начисление {PAY_ID}: дата расчетного периода \"{periodDate.Year}.{periodDate.Month}.{periodDate.Day}\" " +
                                          $"не совпадает с указанной в имени файла \"{PeriodDateFromFileName.Year}.{PeriodDateFromFileName.Month}.{PeriodDateFromFileName.Day}\".");
                }
            }

            return result;
        }
       
        public static int DefineIbzkhPremiseTypeId(int ckuPremiseTypeId)               // трансляция типа помещения ЦКУ в тип помещения ИБ ЖКХ.
        {
            if (ActualCkuPremiseIds.Habitable.Contains(ckuPremiseTypeId))              // ЦКУ-шный id помещения относится к ЖИЛЫМ.
                return (int)PremiseTypeEnum.Habitable;

            if (ActualCkuPremiseIds.Uninhabited.Contains(ckuPremiseTypeId))            // ЦКУ-шный id помещения относится к НЕЖИЛЫМ.
                return (int)PremiseTypeEnum.Uninhabited;

            return -1;     // ЦКУ-шный id помещения имеет значение не из множества допустимых значений.
        }

        public static int DefineIbzkhChargeTypeId(int ID_USL)
        {
            switch(ID_USL)
            {
                case 0:
                    return (int)ChargeTypeEnum.Penalty;
                default:
                    return (int)ChargeTypeEnum.Main;
            }
        }
    }
}