import std.stdio;
import std.conv : to;
import std.range : iota;
import std.array : array;
import std.range : front;
import std.algorithm : map, min, multiSort, splitter;
import std.parallelism : taskPool;
import std.mmfile : MmFile;
import std.string : lineSplitter;
import core.memory : GC;
//import std.datetime : Clock;

void main()
{
    //writeln("Started at ", Clock.currTime);
    auto fh = new MmFile("input.txt", MmFile.Mode.read, 0, null);
    scope(exit) fh.destroy();
    string input = cast(string) fh[];

    GC.disable();
    scope(exit) GC.enable();
    //writeln("File loaded at ", Clock.currTime);

    string[] parts = input.splitChunks(4);
    //writeln("Chunks split at ", Clock.currTime);
    Dataset dataset = taskPool.reduce!(combine)(taskPool.amap!parseChunk(parts));
    //writeln("Dataset ready at ", Clock.currTime);

    size_t cheapestCity = 0;
    foreach(city, cost; dataset.cityCosts)
    {
        if(cost != 0 && cost < dataset.cityCosts[cheapestCity])
        {
            cheapestCity = city;
        }
    }
    //writeln("Cheapest city found at ", Clock.currTime);

    auto fout = File("output.txt", "w");
    fout.writefln!`%s %.2f`(ALL_CITIES[cheapestCity], dataset.cityCosts[cheapestCity]);
    size_t[] cheapestProducts = iota(0, dataset.products[cheapestCity].length).array();
    cheapestProducts.multiSort!(
        (a, b) => dataset.products[cheapestCity][a] < dataset.products[cheapestCity][b],
        (a, b) => ALL_PRODUCTS[a] < ALL_PRODUCTS[b]
    );
    foreach(i, product; cheapestProducts[0 .. 5])
    {
        fout.writefln!`%s %.2f`(ALL_PRODUCTS[product], dataset.products[cheapestCity][product]);
    }
    //writeln("Done at ", Clock.currTime);
}

struct Dataset
{
    double[ALL_CITIES.length] cityCosts;
    double[ALL_PRODUCTS.length][ALL_CITIES.length] products;
}

//we only need to parse up to 100 positive doubles
double parseDouble(immutable string input) pure
{
    double result = 0;
    ulong decimalPosition = input.length - 1;
    foreach(i, c; input)
    {
        if(c == '.')
        {
            decimalPosition = i;
            continue;
        }
        result += c - '0';
        result *= 10;
    }
    return result / 10 / (10 ^^ (input.length - decimalPosition - 1));
}

unittest
{
    assert("123.4".parseDouble() == 123.4);
    assert("123.45".parseDouble() == 123.45);
    assert("12.34".parseDouble() == 12.34);
    assert("12.4".parseDouble() == 12.4);
    assert("1.24".parseDouble() == 1.24);
    assert("13".parseDouble() == 13);
    assert("0".parseDouble() == 0);
    assert("0.0".parseDouble() == 0);
}

Dataset parseChunk(string chunk)
{
    //writeln("parseChunk ", thisTid, " at ", Clock.currTime);
    //scope(exit) writeln("parseChunk finished at ", Clock.currTime);
    double[ALL_CITIES.length] cityCosts = 0;// = new double[](ALL_CITIES.length);
    double[ALL_PRODUCTS.length][ALL_CITIES.length] products;// = new double[][](ALL_CITIES.length, ALL_PRODUCTS.length);
    foreach(c; 0 .. ALL_CITIES.length)
    {
        products[c][] = uint.max;
    }

    foreach(immutable string line; chunk.lineSplitter())
    {
        auto parts = line.splitter(",");
        immutable string city = parts.front();
        parts.popFront();
        immutable string product = parts.front();
        parts.popFront();
        immutable double price = line[city.length + 1 + product.length + 1 .. $].parseDouble();

        size_t cityHash = hashCity(city);
        size_t productHash = hashProduct(product);
        cityCosts[cityHash] += price;
        products[cityHash][productHash] = min(price, products[cityHash][productHash]);
    }

    return Dataset(cityCosts, products);
}

immutable string[] ALL_CITIES = ["Agadir", "Ahfir", "Ait_Melloul", "Akhfenir", "Al_Hoceima", "Aourir", "Arfoud", "Asilah", "Assa", "Azilal", "Azrou", "Bab_Berred", "Bab_Taza", "Ben_guerir", "Beni_Mellal", "Berkane", "Berrechid", "Bir_Anzerane", "Bir_Lehlou", "Bni_Hadifa", "Bouarfa", "Boujdour", "Boulemane", "Béni_Mellal", "Casablanca", "Chefchaouen", "Chichaoua", "Dakhla", "Demnate", "Drarga", "El_Jadida", "Errachidia", "Essaouira", "Fes", "Figuig", "Fquih_Ben_Salah", "Goulmima", "Guelmim", "Guelta_Zemmur", "Guercif", "Guerguerat", "Ifrane", "Imzouren", "Inezgane", "Jerada", "Jorf_El_Melha", "Kalaat_MGouna", "Kenitra", "Khemisset", "Khenifra", "Khouribga", "Ksar_El_Kebir", "Ksar_es_Seghir", "Laayoune", "Larache", "Layoune", "Laâyoune", "Marrakech", "Meknes", "Midar", "Midelt", "Mohammedia", "Moulay_Bousselham", "Nador", "Ouarzazate", "Ouazzane", "Oujda", "Oujda_Angad", "Oulad_Teima", "Rabat", "Safi", "Saidia", "Sale", "Sefrou", "Settat", "Sidi_Bennour", "Sidi_Bouzid", "Sidi_Ifni", "Sidi_Kacem", "Sidi_Slimane", "Skhirate", "Smara", "Souk_Larbaa", "Tafraout", "Tan-Tan", "Tangier", "Taourirt", "Tarfaya", "Taroudant", "Taza", "Temara", "Tetouan", "Tichka", "Tichla", "Tiflet", "Tinghir", "Tiznit", "Youssoufia", "Zagora", "Zemamra", "had_soualem"];

size_t hashCity(immutable string city) pure @safe nothrow
{
    switch(city)
    {
        static foreach(i, c; ALL_CITIES)
        {
            case c: return i;
        }
        default: assert(false, "Unknown city " ~ city);
    }
}

immutable string[] ALL_PRODUCTS = ["Acorn_Squash", "Apple", "Apricot", "Artichoke", "Asparagus", "Avocado", "Banana", "Basil", "Beet", "Bell_Pepper", "Blackberry", "Blueberry", "Bok_Choy", "Broccoli", "Brussels_Sprouts", "Butternut_Squash", "Cabbage", "Cactus_Pear", "Cantaloupe", "Carrot", "Cauliflower", "Celery", "Chard", "Cherry", "Cilantro", "Clementine", "Coconut", "Collard_Greens", "Cranberry", "Cucumber", "Currant", "Date", "Dill", "Dragon_Fruit", "Eggplant", "Endive", "Fig", "Garlic", "Ginger", "Goji_Berry", "Grapefruit", "Grapes", "Green_Beans", "Guava", "Honeydew", "Jackfruit", "Jicama", "Kale", "Kiwano", "Kiwi", "Kohlrabi", "Lemon", "Lettuce", "Lime", "Mango", "Mint", "Nectarine", "Okra", "Onion", "Orange", "Oregano", "Papaya", "Parsley", "Parsnip", "Passion_Fruit", "Peach", "Pear", "Peas", "Persimmon", "Pineapple", "Plantain", "Plum", "Pomegranate", "Potato", "Pumpkin", "Radish", "Raspberry", "Rhubarb", "Rosemary", "Rutabaga", "Sage", "Salsify", "Spinach", "Squash_Blossom", "Starfruit", "Strawberry", "Sweet_Potato", "Thyme", "Tomato", "Turnip", "Watercress", "Watermelon", "Yam", "Zucchini"];

size_t hashProduct(immutable string product) pure @safe nothrow
{
    switch(product)
    {
        static foreach(i, p; ALL_PRODUCTS)
        {
            case p: return i;
        }
        default: assert(false, "Unknown product " ~ product);
    }
}

Dataset combine(Dataset a, Dataset b)
{
    //writeln("combine ", thisTid);
    //scope(exit) writeln("combine finished at ", Clock.currTime);
    foreach(city, cost; b.cityCosts)
    {
        a.cityCosts[city] += cost;
    }
    foreach(city, products; b.products)
    {
        foreach(product, cost; products)
        {
            a.products[city][product] = min(
                a.products[city][product],
                cost
            );
        }
    }
    return a;
}

unittest
{
    import std.algorithm : each;
    auto dataset = Dataset();
    dataset.cityCosts[] = 0;
    dataset.cityCosts[hashCity("Casablanca")] = 12;
    dataset.cityCosts[hashCity("Rabat")] = 11;

    dataset.products.each!((ref p) => p[] = int.max);

    dataset.products[hashCity("Casablanca")][hashProduct("Banana")] = 1;
    dataset.products[hashCity("Casablanca")][hashProduct("Orange")] = 4000;
    dataset.products[hashCity("Rabat")][hashProduct("Orange")] = 3000;
    dataset.products[hashCity("Rabat")][hashProduct("Pear")] = 10;

    auto dataset2 = Dataset();
    dataset2.cityCosts[] = 0;
    dataset2.cityCosts[hashCity("Casablanca")] = 12;
    dataset2.cityCosts[hashCity("Marrakech")] = 11;

    dataset2.products.each!((ref p) => p[] = int.max);

    dataset2.products[hashCity("Casablanca")][hashProduct("Banana")] = 1;
    dataset2.products[hashCity("Casablanca")][hashProduct("Orange")] = 3000;
    dataset2.products[hashCity("Marrakech")][hashProduct("Orange")] = 2000;
    dataset2.products[hashCity("Marrakech")][hashProduct("Pear")] = 12;

    auto actual = combine(dataset, dataset2);
    auto expected = Dataset();
    expected.cityCosts[] = 0;
    expected.cityCosts[hashCity("Casablanca")] = 24;
    expected.cityCosts[hashCity("Marrakech")] = 11;
    expected.cityCosts[hashCity("Rabat")] = 11;

    expected.products.each!((ref p) => p[] = int.max);

    expected.products[hashCity("Casablanca")][hashProduct("Banana")] = 1;
    expected.products[hashCity("Casablanca")][hashProduct("Orange")] = 3000;
    expected.products[hashCity("Rabat")][hashProduct("Orange")] = 3000;
    expected.products[hashCity("Rabat")][hashProduct("Pear")] = 10;
    expected.products[hashCity("Marrakech")][hashProduct("Orange")] = 2000;
    expected.products[hashCity("Marrakech")][hashProduct("Pear")] = 12;

    assert(expected == actual, "combine() test failed");
}

string[] splitChunks(string input, int n)
{
    string[] chunks;
    chunks.reserve(n);
    ulong chunkSize = input.length / n;
    ulong start = 0, end = chunkSize;
    while(end < input.length)
    {
        while(input[end] != '\n')
        {
            end++;
        }
        chunks ~= input[start .. end];
        start = end + 1;
        end += chunkSize;
    }
    chunks ~= input[start .. $];
    return chunks;
}

unittest
{
    string input = "casa,tomato,6.23
casa,tomato,7.23
casa,tomato,8.23
casa,tomato,9.23
casa,potato,4.21
casa,flour,6.24
casa,oil,9.24";

    string[] expected = [
        "casa,tomato,6.23\ncasa,tomato,7.23",
        "casa,tomato,8.23\ncasa,tomato,9.23",
        "casa,potato,4.21\ncasa,flour,6.24",
        "casa,oil,9.24"
    ];
    auto actual = input.splitChunks(4);
    assert(expected == actual);
}
