using System.Collections.Generic;
using Import.API.Tools.ImportData.Importer.Tools.CkuCharge.Validators;

namespace Import.API.Tools.ImportData.Importer.Tools.CkuCharge.Comparers
{

public class PremiseBuildingAddressComparer : IEqualityComparer<CkuPremisesDetail>
{
// :::::::::::::::::::::::::::::::::::::::: Работа. ::::::::::::::::::::::::::::::::::::::::

public bool Equals(CkuPremisesDetail first, CkuPremisesDetail second)
{
    if (first == null && second == null)
        return true;

    if (first == null || second == null)
        return false;

    return first.UL_TP == second.UL_TP 
        && first.UL == second.UL 
        && first.DOM == second.DOM
        && first.KOR == second.KOR;
}

public int GetHashCode(CkuPremisesDetail prem)
{
    return prem.UL_TP.GetHashCode()
         ^ prem.UL.GetHashCode()
         ^ prem.DOM.GetHashCode()
         ^ prem.KOR.GetHashCode();
}
}

}