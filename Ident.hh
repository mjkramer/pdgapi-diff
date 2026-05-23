class Ident {
public:
    Ident(std::vector<SqlVal> values);
    Ident(std::string rendering);
    std::string render();
};

Ident::Ident(std::vector<SqlVal> values) {}
