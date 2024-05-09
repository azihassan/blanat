import std.stdio;
import std.conv : to;
import std.range : iota;
import std.array : array;
import std.algorithm : map, max, min, multiSort, reduce;
import std.parallelism : taskPool;
import std.mmfile : MmFile;
import std.string : indexOf, lastIndexOf;

void main()
{
    //writeln("Started at ", Clock.currTime);
    auto fh = new MmFile("input.txt", MmFile.Mode.read, 0, null);
    scope(exit) fh.destroy();
    string input = cast(string) fh[];

    //writeln("File loaded at ", Clock.currTime);

    string[] parts = input.splitChunks(4);
    //writeln("Chunks split at ", Clock.currTime);
    //Dataset dataset = reduce!(combine)(map!parseChunk(parts));
    Dataset dataset = taskPool.reduce!(combine)(taskPool.amap!parseChunk(parts));
    //Dataset dataset = reduce!(combine)(map!parseChunk(parts));
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
        if(dataset.products[cheapestCity][product] == int.max)
        {
            break;
        }
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
double parseDouble(string chunk, ref size_t index)
{
    double result = 0;
    ulong decimalPosition = 0;
    size_t length = 0;
    while(index < chunk.length && chunk[index] != '\n')
    {
        length++;
        if(chunk[index] == '.')
        {
            decimalPosition = index;
            index++;
            continue;
        }
        result += chunk[index] - '0';
        result *= 10;
        index++;
    }
    if(decimalPosition == 0)
    {
        decimalPosition = length - 1;
    }
    return result / 10 / (10 ^^ (index - decimalPosition - 1));
}

unittest
{
    size_t index = 0;
    assert("123.4\n".parseDouble(index) == 123.4);
    assert(index == 5);

    index = 0;
    assert("123.45\n".parseDouble(index) == 123.45);
    assert(index == 6);

    index = 0;
    assert("12.34\n".parseDouble(index) == 12.34);
    assert(index == 5);

    index = 0;
    assert("12.4\n".parseDouble(index) == 12.4);
    assert(index == 4);

    index = 0;
    assert("1.24\n".parseDouble(index) == 1.24);
    assert(index == 4);

    index = 0;
    //writeln("13\n".parseDouble(index));
    assert("13\n".parseDouble(index) == 13);
    assert(index == 2);

    index = 0;
    assert("0\n".parseDouble(index) == 0);
    assert(index == 1);

    index = 0;
    assert("0.0\n".parseDouble(index) == 0);
    assert(index == 3);
}

//void parseLine(string line, ref double[ALL_CITIES.length] cityCosts, ref double[ALL_PRODUCTS.length][ALL_CITIES.length] products)
//{
//    size_t firstComma = line.indexOf(',');
//    size_t lastComma = line.lastIndexOf(',');
//    immutable string city = line[0 .. firstComma];
//    immutable string product = line[firstComma + 1 .. lastComma];
//    immutable double price = line[lastComma + 1 .. $].parseDouble();
//
//    size_t cityHash = hashCity(city);
//    size_t productHash = hashProduct(product);
//    cityCosts[cityHash] += price;
//    products[cityHash][productHash] = min(price, products[cityHash][productHash]);
//}

Dataset parseChunk(string chunk)
{
    //writeln("parseChunk ", thisTid, " at ", Clock.currTime);
    //scope(exit) writeln("parseChunk finished at ", Clock.currTime);
    double[ALL_CITIES.length] cityCosts = 0;
    double[ALL_PRODUCTS.length][ALL_CITIES.length] products;
    foreach(c; 0 .. ALL_CITIES.length)
    {
        products[c][] = uint.max;
    }

    size_t index = 0;
    char[MAX_CITY_LENGTH] city;
    char[MAX_PRODUCT_LENGTH] product;
    while(index < chunk.length)
    {
        city[0 .. MIN_CITY_LENGTH] = chunk[index .. index + MIN_CITY_LENGTH];
        index += MIN_CITY_LENGTH;
        size_t cityIndex = MIN_CITY_LENGTH;
        while(chunk[index] != ',')
        {
            city[cityIndex++] = chunk[index++];
        }
        size_t cityHash = hashCity(city[0 .. cityIndex]);
        index++;

        product[0 .. MIN_PRODUCT_LENGTH] = chunk[index .. index + MIN_PRODUCT_LENGTH];
        index += MIN_PRODUCT_LENGTH;
        size_t productIndex = MIN_PRODUCT_LENGTH;
        while(chunk[index] != ',')
        {
            product[productIndex++] = chunk[index++];
        }
        size_t productHash = hashProduct(product[0 .. productIndex]);
        index++;

        double price = parseDouble(chunk, index);
        index++;
        //writeln(city[0 .. cityIndex], ",", product[0 .. productIndex], ",", price);

        cityCosts[cityHash] += price;
        products[cityHash][productHash] = min(price, products[cityHash][productHash]);
    }
    //string line = chunk[lineStart .. $];
    //if(line.length > 0)
    //{
    //    line.parseLine(cityCosts, products);
    //}

    return Dataset(cityCosts, products);
}

immutable char[][] ALL_CITIES = ["Agadir", "Ahfir", "Ait_Melloul", "Akhfenir", "Al_Hoceima", "Aourir", "Arfoud", "Asilah", "Assa", "Azilal", "Azrou", "Bab_Berred", "Bab_Taza", "Ben_guerir", "Beni_Mellal", "Berkane", "Berrechid", "Bir_Anzerane", "Bir_Lehlou", "Bni_Hadifa", "Bouarfa", "Boujdour", "Boulemane", "Béni_Mellal", "Casablanca", "Chefchaouen", "Chichaoua", "Dakhla", "Demnate", "Drarga", "El_Jadida", "Errachidia", "Essaouira", "Fes", "Figuig", "Fquih_Ben_Salah", "Goulmima", "Guelmim", "Guelta_Zemmur", "Guercif", "Guerguerat", "Ifrane", "Imzouren", "Inezgane", "Jerada", "Jorf_El_Melha", "Kalaat_MGouna", "Kenitra", "Khemisset", "Khenifra", "Khouribga", "Ksar_El_Kebir", "Ksar_es_Seghir", "Laayoune", "Larache", "Layoune", "Laâyoune", "Marrakech", "Meknes", "Midar", "Midelt", "Mohammedia", "Moulay_Bousselham", "Nador", "Ouarzazate", "Ouazzane", "Oujda", "Oujda_Angad", "Oulad_Teima", "Rabat", "Safi", "Saidia", "Sale", "Sefrou", "Settat", "Sidi_Bennour", "Sidi_Bouzid", "Sidi_Ifni", "Sidi_Kacem", "Sidi_Slimane", "Skhirate", "Smara", "Souk_Larbaa", "Tafraout", "Tan-Tan", "Tangier", "Taourirt", "Tarfaya", "Taroudant", "Taza", "Temara", "Tetouan", "Tichka", "Tichla", "Tiflet", "Tinghir", "Tiznit", "Youssoufia", "Zagora", "Zemamra", "had_soualem"];
enum MIN_CITY_LENGTH = ALL_CITIES.map!(c => c.length).reduce!min();
enum MAX_CITY_LENGTH = ALL_CITIES.map!(c => c.length).reduce!max();
pragma(msg, MIN_CITY_LENGTH);

size_t hashCity(char[] city) pure @safe nothrow
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

immutable char[][] ALL_PRODUCTS = ["Acorn_Squash", "Apple", "Apricot", "Artichoke", "Asparagus", "Avocado", "Banana", "Basil", "Beet", "Bell_Pepper", "Blackberry", "Blueberry", "Bok_Choy", "Broccoli", "Brussels_Sprouts", "Butternut_Squash", "Cabbage", "Cactus_Pear", "Cantaloupe", "Carrot", "Cauliflower", "Celery", "Chard", "Cherry", "Cilantro", "Clementine", "Coconut", "Collard_Greens", "Cranberry", "Cucumber", "Currant", "Date", "Dill", "Dragon_Fruit", "Eggplant", "Endive", "Fig", "Garlic", "Ginger", "Goji_Berry", "Grapefruit", "Grapes", "Green_Beans", "Guava", "Honeydew", "Jackfruit", "Jicama", "Kale", "Kiwano", "Kiwi", "Kohlrabi", "Lemon", "Lettuce", "Lime", "Mango", "Mint", "Nectarine", "Okra", "Onion", "Orange", "Oregano", "Papaya", "Parsley", "Parsnip", "Passion_Fruit", "Peach", "Pear", "Peas", "Persimmon", "Pineapple", "Plantain", "Plum", "Pomegranate", "Potato", "Pumpkin", "Radish", "Raspberry", "Rhubarb", "Rosemary", "Rutabaga", "Sage", "Salsify", "Spinach", "Squash_Blossom", "Starfruit", "Strawberry", "Sweet_Potato", "Thyme", "Tomato", "Turnip", "Watercress", "Watermelon", "Yam", "Zucchini"];
enum MIN_PRODUCT_LENGTH = ALL_PRODUCTS.map!(p => p.length).reduce!min();
enum MAX_PRODUCT_LENGTH = ALL_PRODUCTS.map!(p => p.length).reduce!max();
pragma(msg, MIN_PRODUCT_LENGTH);

size_t hashProduct(char[] product) pure @safe nothrow
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
    dataset.cityCosts[hashCity("Casablanca".dup)] = 12;
    dataset.cityCosts[hashCity("Rabat".dup)] = 11;

    dataset.products.each!((ref p) => p[] = int.max);

    dataset.products[hashCity("Casablanca".dup)][hashProduct("Banana".dup)] = 1;
    dataset.products[hashCity("Casablanca".dup)][hashProduct("Orange".dup)] = 4000;
    dataset.products[hashCity("Rabat".dup)][hashProduct("Orange".dup)] = 3000;
    dataset.products[hashCity("Rabat".dup)][hashProduct("Pear".dup)] = 10;

    auto dataset2 = Dataset();
    dataset2.cityCosts[] = 0;
    dataset2.cityCosts[hashCity("Casablanca".dup)] = 12;
    dataset2.cityCosts[hashCity("Marrakech".dup)] = 11;

    dataset2.products.each!((ref p) => p[] = int.max);

    dataset2.products[hashCity("Casablanca".dup)][hashProduct("Banana".dup)] = 1;
    dataset2.products[hashCity("Casablanca".dup)][hashProduct("Orange".dup)] = 3000;
    dataset2.products[hashCity("Marrakech".dup)][hashProduct("Orange".dup)] = 2000;
    dataset2.products[hashCity("Marrakech".dup)][hashProduct("Pear".dup)] = 12;

    auto actual = combine(dataset, dataset2);
    auto expected = Dataset();
    expected.cityCosts[] = 0;
    expected.cityCosts[hashCity("Casablanca".dup)] = 24;
    expected.cityCosts[hashCity("Marrakech".dup)] = 11;
    expected.cityCosts[hashCity("Rabat".dup)] = 11;

    expected.products.each!((ref p) => p[] = int.max);

    expected.products[hashCity("Casablanca".dup)][hashProduct("Banana".dup)] = 1;
    expected.products[hashCity("Casablanca".dup)][hashProduct("Orange".dup)] = 3000;
    expected.products[hashCity("Rabat".dup)][hashProduct("Orange".dup)] = 3000;
    expected.products[hashCity("Rabat".dup)][hashProduct("Pear".dup)] = 10;
    expected.products[hashCity("Marrakech".dup)][hashProduct("Orange".dup)] = 2000;
    expected.products[hashCity("Marrakech".dup)][hashProduct("Pear".dup)] = 12;

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
