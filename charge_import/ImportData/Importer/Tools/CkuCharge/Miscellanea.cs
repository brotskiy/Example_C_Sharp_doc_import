using System;
using System.Collections.Generic;
using System.Linq;

namespace Import.API.Tools.ImportData.Importer.Tools.CkuCharge
{

public static class Miscellanea
{

public static void InsertOrAdd<TKey, TElem>(Dictionary<TKey, IEnumerable<TElem>> dict, TKey key, IEnumerable<TElem> val)
{
    if (dict.ContainsKey(key))
        dict[key] = dict[key].Concat(val).ToList();
    else
        dict[key] = val;
}

public static IEnumerable<TElem> GetFactorizablyEquivalent<TElem, TKey>(IEnumerable<TElem> collection,
                                                                        Func<TElem, TKey> factorizer,
                                                                        IEnumerable<IEqualityComparer<TElem>> comparers,
                                                                        out IEnumerable<TElem> invalid)
{
    bool IsMultipleHomogeneous(IEnumerable<TElem> incoming)
    {
        foreach (var comparer in comparers)
            if (!IsHomogeneous(incoming, comparer))
                return false;

        return true;
    }

    // сначала группируем пришедшие записи по заданному ключу.
    var groupsByKey = collection
        .GroupBy(factorizer)
        .ToList();

    // оставляем только группировки, которые содержат записи, идентичные по заданным нами критериям.
    collection = Filter(groupsByKey, group => IsMultipleHomogeneous(group), out var invalidGroups)
        .SelectMany(group => group)
        .ToList();

    // забираем все записи, содержащие ошибки.
    invalid = invalidGroups
        .SelectMany(group => group)
        .ToList();

    return collection;
}

public static IEnumerable<TElem> Filter<TElem>(IEnumerable<TElem> collection,
                                               Func<TElem, bool> predicate,
                                               out IEnumerable<TElem> invalid_out)
{
    // выделяем элементы, для которых условие не выполняется.
    invalid_out = collection
        .Where(elem => !predicate(elem))
        .ToList();

    // если есть элементы, для которых не выполняется условие, то фильтруем исходную коллекцию.
    if (invalid_out.Any())
        collection = collection
            .Where(elem => predicate(elem))
            .ToList();
    
    return collection;
}

public static bool IsHomogeneous<TElem>(IEnumerable<TElem> collection, 
                                        IEqualityComparer<TElem> comparer)
{
    if (collection.Distinct(comparer).Count() > 1)    // считаем, что пустая коллекция гомогенна.
        return false;
    
    return true;
}

}

}