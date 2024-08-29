import re
import pathlib
from itertools import islice

from pybtex import errors
from pycldf.sources import Sources
from cldfbench import Dataset as BaseDataset, CLDFSpec


VESELINOVA_2013 = """\
@article{veselinova2013,
    author = {Ljuba Veselinova},
    year = {2013},
    journal = {Rivista di Linguistica},
    volume = {25},
    issue = {1},
    pages = {107-145}
}
"""


def read_fixed_isos(csv_assocs):
    assert csv_assocs[0] == ['NAM_LABEL', 'ID_ISO_A3']
    return {
        label: isocode
        for label, isocode in islice(csv_assocs, 1, None)}


def make_languages(data, fixed_isos, languoids_by_iso):
    return [
        {
            'ID': (iso := fixed_isos.get(row['NAM_LABEL']) or row['ID_ISO_A3']),
            'Name': row['NAM_LABEL'],
            'ISO639P3code': iso,
            'Glottocode': glang.id if (glang := languoids_by_iso.get(iso)) else None,
            'Latitude': glang.latitude if glang else None,
            'Longitude': glang.longitude if glang else None,
        }
        for row in data]


def make_parameters(csv_parameters):
    parameters = {
        parameter['Original_Name']: parameter
        for parameter in csv_parameters}
    for parameter in parameters.values():
        if (grammacodes := parameter.get('Grammacodes')):
            parameter['Grammacodes'] = [
                id_.strip()
                for id_ in grammacodes.split(',')]
    return parameters


def make_codes(csv_codes):
    return {
        (row['Parameter_ID'], row['Original_Name']): row
        for row in csv_codes}


def citation_from_bibtex(bibtex_entry):
    if (authors := bibtex_entry.get('author')):
        names = ' & '.join(a.split(',')[0] for a in authors.split(' and '))
    else:
        names = None
    year = bibtex_entry.get('year')
    return f'{names} {year}'


def make_sources(bibtex_sources):
    return {citation_from_bibtex(entry): entry for entry in bibtex_sources}


def citation_from_authoryear(text):
    if not text:
        return None, None

    if ':' in text:
        citation, pages = text.split(':', maxsplit=1)
    else:
        citation, pages = text, None

    citation = citation.replace('(', '')
    citation = citation.replace(',', '')
    citation = citation.replace('MIchael', 'Michael')
    citation = citation.replace('MIestam', 'Miestam')
    citation = citation.replace('Brandup', 'Brandrup')
    citation = re.sub(r'\s+', ' ', citation)
    citation = citation.replace(' and ', ' & ')

    if 'p.c.' not in citation.replace(' ', ''):
        return citation, pages.strip() if pages else pages
    else:
        return None, None


def make_value(
    row, param_col, params_by_col, codes_by_value, sources_by_citation
):
    citations = []
    if param_col == 'SN':
        citation, pages = citation_from_authoryear(row['Source_SN'])
        if citation in sources_by_citation:
            bibkey = sources_by_citation[citation].id
            citations = [f'{bibkey}[{pages}]' if pages else bibkey]
    elif param_col == 'NegEx_Form':
        citation, pages = citation_from_authoryear(row['Source_NegEx'])
        if citation in sources_by_citation:
            bibkey = sources_by_citation[citation].id
            citations = [f'{bibkey}[{pages}]' if pages else bibkey]
    language_id = row['ID_ISO_A3']
    original_value = row[param_col].strip()
    parameter = params_by_col[param_col]
    parameter_id = parameter['ID']
    code = codes_by_value.get((parameter_id, original_value))
    return {
        'ID': f'{language_id}-{param_col}',
        'Code_ID': code['ID'] if code else '',
        'Value': code['Name'] if code else original_value,
        'Language_ID': language_id,
        'Parameter_ID': parameter_id,
        'Source': citations,
    }


def make_values(data, params_by_col, codes_by_value, sources_by_citation):
    parameter_columns = ['SN', 'NegEx_Form', 'NegExType_general_classificaiton']
    return [
        make_value(row, param_col, params_by_col, codes_by_value, sources_by_citation)
        for row in data
        for param_col in parameter_columns]


def update_cldf_schema(cldf):
    cldf.add_component('LanguageTable')
    cldf.add_component(
        'ParameterTable',
        {
            'name': 'Grammacodes',
            'separator': ';',
        })
    cldf.add_component('CodeTable', 'Map_Icon')


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "veselinovanegex"

    def cldf_specs(self):
        # A dataset must declare all CLDF sets it creates.
        return CLDFSpec(module='StructureDataset', dir=self.cldf_dir)

    def cmd_download(self, args):
        pass

    def cmd_makecldf(self, args):
        raw_data = list(self.raw_dir.read_csv('NegEx_CLDF.csv', dicts=True))

        languoids_by_iso = {
            languoid.iso: languoid
            for languoid in args.glottolog.api.languoids()
            if languoid.iso}
        fixed_isos = read_fixed_isos(self.etc_dir.read_csv('languages.csv'))
        languages = make_languages(raw_data, fixed_isos, languoids_by_iso)

        params_by_col = make_parameters(
            self.etc_dir.read_csv('parameters.csv', dicts=True))
        codes_by_value = make_codes(self.etc_dir.read_csv('codes.csv', dicts=True))

        errors.strict = False
        sources_by_citation = make_sources(Sources.from_file(self.raw_dir / 'NegEx_bib.txt'))

        values = make_values(
            raw_data, params_by_col, codes_by_value, sources_by_citation)

        update_cldf_schema(args.writer.cldf)

        args.writer.objects['LanguageTable'] = languages
        args.writer.objects['ParameterTable'] = params_by_col.values()
        args.writer.objects['CodeTable'] = codes_by_value.values()
        args.writer.objects['ValueTable'] = values

        args.writer.cldf.add_sources(*sources_by_citation.values())
        args.writer.cldf.add_sources(VESELINOVA_2013)
