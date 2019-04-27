defmodule AttributeRepositoryMnesiaTest.Search do
  use ExUnit.Case

  @init_opts []
  @run_opts [instance: :test]

  setup_all do
    AttributeRepositoryMnesia.install(@run_opts, @init_opts)

    :mnesia.start()

    for i <- 1..9 do
      AttributeRepositoryMnesia.put(
        i,
        %{
          "str" => ?a..?z |> Enum.chunk_every(3) |> Enum.at(i - 1) |> List.to_string(),
          "bool" => rem(i, 3) == 0,
          "float" => i * 1.0,
          "int" => i,
          "datetime" => DateTime.from_iso8601("#{2000 + i}-01-23T23:50:07Z") |> elem(1),
          "complex" => %{
            "str" => ?a..?z |> Enum.chunk_every(3) |> Enum.at(i - 1) |> List.to_string(),
            "bool" => rem(i, 3) == 0,
            "float" => i * 1.0,
            "int" => i,
            "datetime" => DateTime.from_iso8601("#{2000 + i}-01-23T23:50:07Z") |> elem(1),
          }
        },
        @run_opts)
    end

    AttributeRepositoryMnesia.modify(3,
                                     [
                                       {:add, "multval", "Hi"},
                                       {:add, "multval", "Salut"},
                                       {:add, "multval", "Привет"}
                                     ], @run_opts)

    AttributeRepositoryMnesia.modify(4, [{:add, "multval", "Hi"},
                                         {:add, "multval", "Salut"}], @run_opts)

    AttributeRepositoryMnesia.modify(1, [{:add, "nil_attr", nil},
                                         {:add, "nil_attr_complex", %{"nil_attr" => nil, "not_nil_attr" => "a value"}}],
                                         @run_opts)
  end

  # and/or operators

  test "and 1" do
    assert ordered_ids(search('int ge 5 and int lt 8')) == [5, 6, 7]
  end

  test "and 2" do
    assert ordered_ids(search('int gt 3 and datetime ge "2006-01-23T23:50:07Z"')) == [6, 7, 8, 9]
  end

  test "and 3" do
    assert ordered_ids(search('bool eq true and datetime lt "2020-04-23T12:25:13Z"')) == [3, 6, 9]
  end

  test "and 4" do
    assert ordered_ids(search('bool eq false and float ge 4.0 and int lt 9')) == [4, 5, 7, 8]
  end

  test "or 1" do
    assert ordered_ids(search('int lt 5 or int ge 8')) == [1, 2, 3, 4, 8, 9]
  end

  test "or 2" do
    assert ordered_ids(search('int lt 3 or datetime gt "2006-01-23T23:50:07Z"')) == [1, 2, 7, 8, 9]
  end

  test "or 3" do
    assert ordered_ids(search('bool eq false or datetime ne "2006-01-23T23:50:07Z"')) == [1, 2, 3, 4, 5, 7, 8, 9]
  end

  test "or 4" do
    assert ordered_ids(search('bool eq true or float lt 2.0 or int ge 8')) == [1, 3, 6, 8, 9]
  end

  test "and & or 1" do
    assert ordered_ids(search('bool eq true or float lt 2.0 and int ge 8')) == [3, 6, 9]
  end

  test "and & or 2" do
    assert ordered_ids(search('bool eq true and float lt 2.0 or int ge 8')) == [8, 9]
  end

  test "and & or 3" do
    assert ordered_ids(search('str eq "jkl" or str eq "mno" or str eq "yz" and float ne 3.0')) == [4, 5, 9]
  end

  test "and & or 4" do
    assert ordered_ids(search('int le 4 and bool eq false or float gt 2.0 and int lt 6')) == [1, 2, 3, 4, 5]
  end

  # not operator

  test "not 1" do
    assert ordered_ids(search('not (bool eq true)')) == [1, 2, 4, 5, 7, 8]
  end

  test "not 2" do
    assert ordered_ids(search('not (float gt 3.5)')) == [1, 2, 3]
  end

  test "not 3" do
    assert ordered_ids(search('not (not (float gt 3.5))')) == [4, 5, 6, 7, 8, 9]
  end

  test "not 4" do
    assert ordered_ids(search('not (multval pr)')) == [1, 2, 5, 6, 7, 8, 9]
  end

  test "not 5" do
    assert ordered_ids(search('not (complex.bool eq false)')) == [3, 6, 9]
  end

  test "not 6" do
    assert ordered_ids(search('not (complex[bool eq false or int eq 3])')) == [6, 9]
  end

  # eq operator

  test "search eq string" do
    assert ordered_ids(search('str eq "def"')) == [2]
  end

  test "search eq bool" do
    assert ordered_ids(search('bool eq true')) == [3, 6, 9]
  end

  test "search eq float" do
    assert ordered_ids(search('float eq 5.0e0')) == [5]
  end

  test "search eq integer" do
    assert ordered_ids(search('int eq 4')) == [4]
  end

  test "search eq date" do
    assert ordered_ids(search('datetime eq "2008-01-23T23:50:07Z"')) == [8]
  end

  test "search eq string in complex" do
    assert ordered_ids(search('complex.str eq "def"')) == [2]
  end

  test "search eq bool in complex" do
    assert ordered_ids(search('complex.bool eq true')) == [3, 6, 9]
  end

  test "search eq float in complex" do
    assert ordered_ids(search('complex.float eq 5.0e0')) == [5]
  end

  test "search eq integer in complex" do
    assert ordered_ids(search('complex.int eq 4')) == [4]
  end

  test "search eq date in complex" do
    assert ordered_ids(search('complex.datetime eq "2008-01-23T23:50:07Z"')) == [8]
  end

  test "search eq string in complex - valuepath" do
    assert ordered_ids(search('complex[str eq "def"]')) == [2]
  end

  test "search eq bool in complex - valuepath" do
    assert ordered_ids(search('complex[bool eq true]')) == [3, 6, 9]
  end

  test "search eq float in complex - valuepath" do
    assert ordered_ids(search('complex[float eq 5.0e0]')) == [5]
  end

  test "search eq integer in complex - valuepath" do
    assert ordered_ids(search('complex[int eq 4]')) == [4]
  end

  test "search eq date in complex - valuepath" do
    assert ordered_ids(search('complex[datetime eq "2008-01-23T23:50:07Z"]')) == [8]
  end

  # ne operator

  test "search ne string" do
    assert ordered_ids(search('str ne "def"')) == [1, 3, 4, 5, 6, 7, 8, 9]
  end

  test "search ne bool" do
    assert ordered_ids(search('bool ne true')) == [1, 2, 4, 5, 7, 8]
  end

  test "search ne float" do
    assert ordered_ids(search('float ne 5.0e0')) == [1, 2, 3, 4, 6, 7, 8, 9]
  end

  test "search ne integer" do
    assert ordered_ids(search('int ne 4')) == [1, 2, 3, 5, 6, 7, 8, 9]
  end

  test "search ne date" do
    assert ordered_ids(search('datetime ne "2008-01-23T23:50:07Z"')) == [1, 2, 3, 4, 5, 6, 7, 9]
  end

  test "search ne string in complex" do
    assert ordered_ids(search('complex.str ne "def"')) == [1, 3, 4, 5, 6, 7, 8, 9]
  end

  test "search ne bool in complex" do
    assert ordered_ids(search('complex.bool ne true')) == [1, 2, 4, 5, 7, 8]
  end

  test "search ne float in complex" do
    assert ordered_ids(search('complex.float ne 5.0e0')) == [1, 2, 3, 4, 6, 7, 8, 9]
  end

  test "search ne integer in complex" do
    assert ordered_ids(search('complex.int ne 4')) == [1, 2, 3, 5, 6, 7, 8, 9]
  end

  test "search ne date in complex" do
    assert ordered_ids(search('complex.datetime ne "2008-01-23T23:50:07Z"')) == [1, 2, 3, 4, 5, 6, 7, 9]
  end

  test "search ne string in complex - valuepath" do
    assert ordered_ids(search('complex[str ne "def"]')) == [1, 3, 4, 5, 6, 7, 8, 9]
  end

  test "search ne bool in complex - valuepath" do
    assert ordered_ids(search('complex[bool ne true]')) == [1, 2, 4, 5, 7, 8]
  end

  test "search ne float in complex - valuepath" do
    assert ordered_ids(search('complex[float ne 5.0e0]')) == [1, 2, 3, 4, 6, 7, 8, 9]
  end

  test "search ne integer in complex - valuepath" do
    assert ordered_ids(search('complex[int ne 4]')) == [1, 2, 3, 5, 6, 7, 8, 9]
  end

  test "search ne date in complex - valuepath" do
    assert ordered_ids(search('complex[datetime ne "2008-01-23T23:50:07Z"]')) == [1, 2, 3, 4, 5, 6, 7, 9]
  end

  # gt operator

  test "search gt string" do
    assert ordered_ids(search('str gt "def"')) == [3, 4, 5, 6, 7, 8, 9]
  end

  test "search gt bool" do
    assert {:error, _} = search('bool gt true')
  end

  test "search gt float" do
    assert ordered_ids(search('float gt 5.0e0')) == [6, 7, 8, 9]
  end

  test "search gt integer" do
    assert ordered_ids(search('int gt 4')) == [5, 6, 7, 8, 9]
  end

  test "search gt date" do
    assert ordered_ids(search('datetime gt "2008-01-23T23:50:07Z"')) == [9]
  end

  test "search gt string in complex" do
    assert ordered_ids(search('complex.str gt "def"')) == [3, 4, 5, 6, 7, 8, 9]
  end

  test "search gt bool in complex" do
    assert {:error, _} = search('complex.bool gt true')
  end

  test "search gt float in complex" do
    assert ordered_ids(search('complex.float gt 5.0e0')) == [6, 7, 8, 9]
  end

  test "search gt integer in complex" do
    assert ordered_ids(search('complex.int gt 4')) == [5, 6, 7, 8, 9]
  end

  test "search gt date in complex" do
    assert ordered_ids(search('complex.datetime gt "2008-01-23T23:50:07Z"')) == [9]
  end

  test "search gt string in complex - valuepath" do
    assert ordered_ids(search('complex[str gt "def"]')) == [3, 4, 5, 6, 7, 8, 9]
  end

  test "search gt bool in complex - valuepath" do
    assert {:error, _} = search('complex[bool gt true]')
  end

  test "search gt float in complex - valuepath" do
    assert ordered_ids(search('complex[float gt 5.0e0]')) == [6, 7, 8, 9]
  end

  test "search gt integer in complex - valuepath" do
    assert ordered_ids(search('complex[int gt 4]')) == [5, 6, 7, 8, 9]
  end

  test "search gt date in complex - valuepath" do
    assert ordered_ids(search('complex[datetime gt "2008-01-23T23:50:07Z"]')) == [9]
  end

  # ge operator

  test "search ge string" do
    assert ordered_ids(search('str ge "def"')) == [2, 3, 4, 5, 6, 7, 8, 9]
  end

  test "search ge bool" do
    assert {:error, _} = search('bool ge true')
  end

  test "search ge float" do
    assert ordered_ids(search('float ge 5.0e0')) == [5, 6, 7, 8, 9]
  end

  test "search ge integer" do
    assert ordered_ids(search('int ge 4')) == [4, 5, 6, 7, 8, 9]
  end

  test "search ge date" do
    assert ordered_ids(search('datetime ge "2008-01-23T23:50:07Z"')) == [8, 9]
  end

  test "search ge string in complex" do
    assert ordered_ids(search('complex.str ge "def"')) == [2, 3, 4, 5, 6, 7, 8, 9]
  end

  test "search ge bool in complex" do
    assert {:error, _} = search('complex.bool ge true')
  end

  test "search ge float in complex" do
    assert ordered_ids(search('complex.float ge 5.0e0')) == [5, 6, 7, 8, 9]
  end

  test "search ge integer in complex" do
    assert ordered_ids(search('complex.int ge 4')) == [4, 5, 6, 7, 8, 9]
  end

  test "search ge date in complex" do
    assert ordered_ids(search('complex.datetime ge "2008-01-23T23:50:07Z"')) == [8, 9]
  end

  test "search ge string in complex - valuepath" do
    assert ordered_ids(search('complex[str ge "def"]')) == [2, 3, 4, 5, 6, 7, 8, 9]
  end

  test "search ge bool in complex - valuepath" do
    assert {:error, _} = search('complex[bool ge true]')
  end

  test "search ge float in complex - valuepath" do
    assert ordered_ids(search('complex[float ge 5.0e0]')) == [5, 6, 7, 8, 9]
  end

  test "search ge integer in complex - valuepath" do
    assert ordered_ids(search('complex[int ge 4]')) == [4, 5, 6, 7, 8, 9]
  end

  test "search ge date in complex - valuepath" do
    assert ordered_ids(search('complex[datetime ge "2008-01-23T23:50:07Z"]')) == [8, 9]
  end

  # lt operator

  test "search lt string" do
    assert ordered_ids(search('str lt "def"')) == [1]
  end

  test "search lt bool" do
    assert {:error, _} = search('bool lt true')
  end

  test "search lt float" do
    assert ordered_ids(search('float lt 5.0e0')) == [1, 2, 3, 4]
  end

  test "search lt integer" do
    assert ordered_ids(search('int lt 4')) == [1, 2, 3]
  end

  test "search lt date" do
    assert ordered_ids(search('datetime lt "2008-01-23T23:50:07Z"')) == [1, 2, 3, 4, 5, 6, 7]
  end

  test "search lt string in complex" do
    assert ordered_ids(search('complex.str lt "def"')) == [1]
  end

  test "search lt bool in complex" do
    assert {:error, _} = search('complex.bool lt true')
  end

  test "search lt float in complex" do
    assert ordered_ids(search('complex.float lt 5.0e0')) == [1, 2, 3, 4]
  end

  test "search lt integer in complex" do
    assert ordered_ids(search('complex.int lt 4')) == [1, 2, 3]
  end

  test "search lt date in complex" do
    assert ordered_ids(search('complex.datetime lt "2008-01-23T23:50:07Z"')) == [1, 2, 3, 4, 5, 6, 7]
  end

  test "search lt string in complex - valuepath" do
    assert ordered_ids(search('complex[str lt "def"]')) == [1]
  end

  test "search lt bool in complex - valuepath" do
    assert {:error, _} = search('complex[bool lt true]')
  end

  test "search lt float in complex - valuepath" do
    assert ordered_ids(search('complex[float lt 5.0e0]')) == [1, 2, 3, 4]
  end

  test "search lt integer in complex - valuepath" do
    assert ordered_ids(search('complex[int lt 4]')) == [1, 2, 3]
  end

  test "search lt date in complex - valuepath" do
    assert ordered_ids(search('complex[datetime lt "2008-01-23T23:50:07Z"]')) == [1, 2, 3, 4, 5, 6, 7]
  end

  # le operator

  test "search le string" do
    assert ordered_ids(search('str le "def"')) == [1, 2]
  end

  test "search le bool" do
    assert {:error, _} = search('bool le true')
  end

  test "search le float" do
    assert ordered_ids(search('float le 5.0e0')) == [1, 2, 3, 4, 5]
  end

  test "search le integer" do
    assert ordered_ids(search('int le 4')) == [1, 2, 3, 4]
  end

  test "search le date" do
    assert ordered_ids(search('datetime le "2008-01-23T23:50:07Z"')) == [1, 2, 3, 4, 5, 6, 7, 8]
  end

  test "search le string in complex" do
    assert ordered_ids(search('complex.str le "def"')) == [1, 2]
  end

  test "search le bool in complex" do
    assert {:error, _} = search('complex.bool le true')
  end

  test "search le float in complex" do
    assert ordered_ids(search('complex.float le 5.0e0')) == [1, 2, 3, 4, 5]
  end

  test "search le integer in complex" do
    assert ordered_ids(search('complex.int le 4')) == [1, 2, 3, 4]
  end

  test "search le date in complex" do
    assert ordered_ids(search('complex.datetime le "2008-01-23T23:50:07Z"')) == [1, 2, 3, 4, 5, 6, 7, 8]
  end

  test "search le string in complex - valuepath" do
    assert ordered_ids(search('complex[str le "def"]')) == [1, 2]
  end

  test "search le bool in complex - valuepath" do
    assert {:error, _} = search('complex[bool le true]')
  end

  test "search le float in complex - valuepath" do
    assert ordered_ids(search('complex[float le 5.0e0]')) == [1, 2, 3, 4, 5]
  end

  test "search le integer in complex - valuepath" do
    assert ordered_ids(search('complex[int le 4]')) == [1, 2, 3, 4]
  end

  test "search le date in complex - valuepath" do
    assert ordered_ids(search('complex[datetime le "2008-01-23T23:50:07Z"]')) == [1, 2, 3, 4, 5, 6, 7, 8]
  end

  # pr operator

  test "search pr no attribute" do
    assert ordered_ids(search('nonexistant pr')) == []
  end

  test "search pr attribute is nil" do
    assert ordered_ids(search('nil_attr pr')) == []
  end

  test "search pr attribute present" do
    assert ordered_ids(search('float pr')) == [1, 2, 3, 4, 5, 6, 7, 8, 9]
  end

  test "search pr no attribute complex" do
    assert ordered_ids(search('nil_attr_complex.nonexistant pr')) == []
  end

  test "search pr attribute is nil complex" do
    assert ordered_ids(search('nil_attr_complex.nil_attr pr')) == []
  end

  test "search pr attribute present complex" do
    assert ordered_ids(search('complex.float pr')) == [1, 2, 3, 4, 5, 6, 7, 8, 9]
  end

  test "search pr no attribute complex - valuepath" do
    assert ordered_ids(search('nil_attr_complex[nonexistant pr]')) == []
  end

  test "search pr attribute is nil complex - valuepath" do
    assert ordered_ids(search('nil_attr_complex[nil_attr pr]')) == []
  end

  test "search pr attribute present complex - valuepath" do
    assert ordered_ids(search('complex[float pr]')) == [1, 2, 3, 4, 5, 6, 7, 8, 9]
  end

  # valuePath :and and :or operator tests

  test "search valuepath and operator 1" do
    assert ordered_ids(search('complex[str eq "abc" and int eq 1]')) == [1]
  end

  test "search valuepath and operator 2" do
    assert ordered_ids(search('complex[int ge 5 and bool eq false]')) == [5, 7, 8]
  end

  test "search valuepath and operator 3" do
    assert ordered_ids(search('complex[str gt "abc" and int lt 9 and bool eq true]')) == [3, 6]
  end

  test "search valuepath and operator 4" do
    assert ordered_ids(search('complex[str gt "abc" and int lt 9 or bool eq true]')) == [2, 3, 4, 5, 6, 7, 8, 9]
  end

  test "search valuepath and operator 5" do
    assert ordered_ids(search('complex[float gt 8.0 or int lt 9 and bool eq true]')) == [3, 6, 9]
  end

  #test "search valuepath and operator 6" do
  #  assert ordered_ids(search('complex[(float gt 8.0 or int lt 9) and bool eq true]')) == []
  #end

  test "search valuepath and operator 7" do
    assert ordered_ids(search('nil_attr_complex[not_nil_attr pr] and bool eq false')) == [1]
  end

  # valuePath :not operator tests

  # unsupported by this implementation

  #test "search valuepath not operator 1" do
  #  assert ordered_ids(search('complex[not (str eq "abc")]')) == [2, 3, 4, 5, 6, 7, 8, 9]
  #end

  # multivalued test

  test "multivalued attr 1" do
    assert ordered_ids(search('multval eq "Salut"')) == [3, 4]
  end

  test "multivalued attr 2" do
    assert ordered_ids(search('multval eq "Salut" and multval eq "Привет"')) == [3]
  end

  test "multivalued attr 3" do
    assert ordered_ids(search('multval eq "Salut" or multval eq "Привет"')) == [3, 4]
  end

  # helper functions

  defp search(req) do
    {_, parsed} = :filter.parse(:filter_lexer.string(req) |> elem(1))

    AttributeRepositoryMnesia.search(parsed, :all, [instance: :test])
  end

  def ordered_ids(search_result_list) do
    search_result_list
    |> Enum.map(fn {resource_id, _resource} -> resource_id end)
    |> Enum.sort()
  end
end
