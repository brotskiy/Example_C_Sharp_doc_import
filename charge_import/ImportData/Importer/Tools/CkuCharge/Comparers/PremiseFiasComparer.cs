using System.Collections.Generic;
using Import.API.Tools.ImportData.Importer.Tools.CkuCharge.Validators;

namespace Import.API.Tools.ImportData.Importer.Tools.CkuCharge.Comparers
{

public class PremiseFiasComparer : IEqualityComparer<CkuPremisesDetail>
{
// :::::::::::::::::::::::::::::::::::::::: Работа. ::::::::::::::::::::::::::::::::::::::::

public bool Equals(CkuPremisesDetail first, CkuPremisesDetail second)
{
    if (first == null && second == null)
        return true;

    if (first == null || second == null)
        return false;

    return first.FiasId == second.FiasId;
}

public int GetHashCode(CkuPremisesDetail prem)
{
    return prem.FiasId.GetHashCode();
}
}

}