# AttributeRepositoryMnesia

Mnesia implementation of `AttributeRepository`

## Installation

```elixir
def deps do
  [
    {:attribute_repository_mnesia, github: "tanguilp/attribute_repository_mnesia", tag: "v0.1.0"}
  ]
end
```

## Usage

`run_opts`:
- `:instance`: the instance name (`Atom.t()`)

`init_opts`:
- `:mnesia_config`: mnesia create table's config parameters

When calling the `AttributeRepositoryMnesia.install/2` function, a new bag table whose name is
`run_opts[:instance]` is created.

## Example

```elixir
iex> run_opts = [instance: :users]
[instance: :users]
iex> init_opts = []
[]
iex> AttributeRepositoryMnesia.install(run_opts, init_opts)

18:15:28.290 [info]  Application mnesia exited: :stopped

18:15:28.315 [error] Elixir.AttributeRepositoryMnesia: failed to create table of instance users (reason: {:already_exists, :users})
{:error, {:already_exists, :users}}
iex> AttributeRepositoryMnesia.put("DKO77TT652NZHXX3WM3ZJBFIC4", %{"first_name" => "Claude", "last_name" => "Leblanc", "shoe_size" => 43, "subscription_date" => DateTime.from_iso8601("2014-06-13T04:42:34Z") |> elem(1)}, run_opts)
{:ok,
 %{
   "first_name" => "Claude",
   "last_name" => "Leblanc",
   "shoe_size" => 43,
   "subscription_date" => #DateTime<2014-06-13 04:42:34Z>
 }}
iex>
18:15:28.320 [debug] Elixir.AttributeRepositoryMnesia: written `%{"first_name" => "Claude", "last_name" => "Leblanc", "shoe_size" => 43, "subscription_date" => #DateTime<2014-06-13 04:42:34Z>}` for resource_id `"DKO77TT652NZHXX3WM3ZJBFIC4"` of instance users
AttributeRepositoryMnesia.put("SGKNRFHMBSKGRVCW4SIJAZMYLE", %{"first_name" => "Xiao", "last_name" => "Ming", "shoe_size" => 36, "subscription_date" => DateTime.from_iso8601("2015-01-29T10:49:58Z") |> elem(1)}, run_opts)

18:15:28.323 [debug] Elixir.AttributeRepositoryMnesia: written `%{"first_name" => "Xiao", "last_name" => "Ming", "shoe_size" => 36, "subscription_date" => #DateTime<2015-01-29 10:49:58Z>}` for resource_id `"SGKNRFHMBSKGRVCW4SIJAZMYLE"` of instance users
{:ok,
 %{
   "first_name" => "Xiao",
   "last_name" => "Ming",
   "shoe_size" => 36,
   "subscription_date" => #DateTime<2015-01-29 10:49:58Z>
 }}
iex> AttributeRepositoryMnesia.put("7WRQL4EAKW27C5BEFF3JDGXBTA", %{"first_name" => "Tomoaki", "last_name" => "Takapamate", "shoe_size" => 34, "subscription_date" => DateTime.from_iso8601("2019-10-13T23:22:51Z") |> elem(1)}, run_opts)

18:15:28.326 [debug] Elixir.AttributeRepositoryMnesia: written `%{"first_name" => "Tomoaki", "last_name" => "Takapamate", "shoe_size" => 34, "subscription_date" => #DateTime<2019-10-13 23:22:51Z>}` for resource_id `"7WRQL4EAKW27C5BEFF3JDGXBTA"` of instance users
{:ok,
 %{
   "first_name" => "Tomoaki",
   "last_name" => "Takapamate",
   "shoe_size" => 34,
   "subscription_date" => #DateTime<2019-10-13 23:22:51Z>
 }}
iex> AttributeRepositoryMnesia.put("WCJBCL7SC2THS7TSRXB2KZH7OQ", %{"first_name" => "Narivelo", "last_name" => "Rajaonarimanana", "shoe_size" => 41, "subscription_date" => DateTime.from_iso8601("2017-06-06T21:01:43Z") |> elem(1), "newsletter_subscribed" => false}, run_opts)

18:15:28.329 [debug] Elixir.AttributeRepositoryMnesia: written `%{"first_name" => "Narivelo", "last_name" => "Rajaonarimanana", "newsletter_subscribed" => false, "shoe_size" => 41, "subscription_date" => #DateTime<2017-06-06 21:01:43Z>}` for resource_id `"WCJBCL7SC2THS7TSRXB2KZH7OQ"` of instance users
{:ok,
 %{
   "first_name" => "Narivelo",
   "last_name" => "Rajaonarimanana",
   "newsletter_subscribed" => false,
   "shoe_size" => 41,
   "subscription_date" => #DateTime<2017-06-06 21:01:43Z>
 }}
iex> AttributeRepositoryMnesia.put("MQNL5ASVNLWZTLJA4MDGHKEXOQ", %{"first_name" => "Hervé", "last_name" => "Le Troadec", "shoe_size" => 48, "subscription_date" => DateTime.from_iso8601("2017-10-19T12:07:03Z") |> elem(1)}, run_opts)

18:15:28.333 [debug] Elixir.AttributeRepositoryMnesia: written `%{"first_name" => "Hervé", "last_name" => "Le Troadec", "shoe_size" => 48, "subscription_date" => #DateTime<2017-10-19 12:07:03Z>}` for resource_id `"MQNL5ASVNLWZTLJA4MDGHKEXOQ"` of instance users
{:ok,
 %{
   "first_name" => "Hervé",
   "last_name" => "Le Troadec",
   "shoe_size" => 48,
   "subscription_date" => #DateTime<2017-10-19 12:07:03Z>
 }}
iex> AttributeRepositoryMnesia.put("Y4HKZMJ3K5A7IMZFZ5O3O56VC4", %{"first_name" => "Lisa", "last_name" => "Santana", "shoe_size" => 33, "subscription_date" => DateTime.from_iso8601("2014-08-30T13:45:45Z") |> elem(1), "newsletter_subscribed" => true}, run_opts)

18:15:28.336 [debug] Elixir.AttributeRepositoryMnesia: written `%{"first_name" => "Lisa", "last_name" => "Santana", "newsletter_subscribed" => true, "shoe_size" => 33, "subscription_date" => #DateTime<2014-08-30 13:45:45Z>}` for resource_id `"Y4HKZMJ3K5A7IMZFZ5O3O56VC4"` of instance users
{:ok,
 %{
   "first_name" => "Lisa",
   "last_name" => "Santana",
   "newsletter_subscribed" => true,
   "shoe_size" => 33,
   "subscription_date" => #DateTime<2014-08-30 13:45:45Z>
 }}
iex> AttributeRepositoryMnesia.put("4D3FB7C89DC04C808CC756151C", %{"first_name" => "Bigfoot", "shoe_size" => 104, "subscription_date" => DateTime.from_iso8601("1914-10-10T03:42:01Z") |> elem(1)}, run_opts)

18:15:28.339 [debug] Elixir.AttributeRepositoryMnesia: written `%{"first_name" => "Bigfoot", "shoe_size" => 104, "subscription_date" => #DateTime<1914-10-10 03:42:01Z>}` for resource_id `"4D3FB7C89DC04C808CC756151C"` of instance users
{:ok,
 %{
   "first_name" => "Bigfoot",
   "shoe_size" => 104,
   "subscription_date" => #DateTime<1914-10-10 03:42:01Z>
 }}
iex> AttributeRepositoryMnesia.get("WCJBCL7SC2THS7TSRXB2KZH7OQ", :all, run_opts)
{:ok,
 %{
   "first_name" => "Narivelo",
   "last_name" => "Rajaonarimanana",
   "newsletter_subscribed" => false,
   "shoe_size" => 41,
   "subscription_date" => #DateTime<2017-06-06 21:01:43Z>
 }}
iex> AttributeRepositoryMnesia.search(~s(shoe_size le 40), :all, run_opts)
[
  {"SGKNRFHMBSKGRVCW4SIJAZMYLE",
   %{
     "first_name" => "Xiao",
     "last_name" => "Ming",
     "shoe_size" => 36,
     "subscription_date" => #DateTime<2015-01-29 10:49:58Z>
   }},
  {"Y4HKZMJ3K5A7IMZFZ5O3O56VC4",
   %{
     "first_name" => "Lisa",
     "last_name" => "Santana",
     "newsletter_subscribed" => true,
     "shoe_size" => 33,
     "subscription_date" => #DateTime<2014-08-30 13:45:45Z>
   }},
  {"7WRQL4EAKW27C5BEFF3JDGXBTA",
   %{
     "first_name" => "Tomoaki",
     "last_name" => "Takapamate",
     "shoe_size" => 34,
     "subscription_date" => #DateTime<2019-10-13 23:22:51Z>
   }}
]

```

## Resource id

The resource id of the `AttributeRepositoryMnesia` implementation is an arbitrary `String.t()`.

## Run options

The `AttributeRepository.run_opts()` for this module are the following:
- `:instance`: instance name (an `atom()`), **mandatory**

## Supported behaviours

- [x] `AttributeRepository.Install`
- [x] `AttributeRepository.Read`
- [x] `AttributeRepository.Write`
- [x] `AttributeRepository.Search`

## Supported attribute types

### Data types

- [x] `String.t()`
- [x] `boolean()`
- [x] `float()`
- [x] `integer()`
- [x] `DateTime.t()`
- [x] `AttributeRepository.binary_data()`
- [ ] `AttributeRepository.ref()`
- [x] `nil`
- [x] `AttributeRepository.object_attribute()` or *complex attribute*

### Cardinality

- [x] Singular attributes
- [x] Multi-valued attributes

## Search support

### Logical operators

- [x] `and`
- [x] `or`
- [x] `not`
  - except in `attr[...]` expressions

### Compare operators

- [x] `eq`
- [x] `ne`
- [x] `gt`
- [x] `ge`
- [x] `lt`
- [x] `le`
- [x] `pr`
- [ ] `sw`
- [ ] `ew`
- [ ] `co`
