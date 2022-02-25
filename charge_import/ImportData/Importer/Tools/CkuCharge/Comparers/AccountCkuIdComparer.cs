using System.Collections.Generic;
using Import.API.Tools.ImportData.Importer.Tools.CkuCharge.Validators;

namespace Import.API.Tools.ImportData.Importer.Tools.CkuCharge.Comparers
{

public class AccountCkuIdComparer : IEqualityComparer<CkuAccountDetail>
{
// :::::::::::::::::::::::::::::::::::::::: Работа. ::::::::::::::::::::::::::::::::::::::::

public bool Equals(CkuAccountDetail first, CkuAccountDetail second)
{
    if (first == null && second == null)
        return true;

    if (first == null || second == null)
        return false;

    return first.CkuId == second.CkuId;
}

public int GetHashCode(CkuAccountDetail acc)
{
    return acc.CkuId.GetHashCode();
}

}

}