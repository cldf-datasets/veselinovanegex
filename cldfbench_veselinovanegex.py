import pathlib
import re
import sys
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

    return citation, pages.strip() if pages else pages


def make_value(
    row, param_col, params_by_col, codes_by_value, sources_by_citation
):
    language_id = row['ID_ISO_A3']

    if param_col == 'SN':
        original_source = row['Source_SN']
    elif param_col == 'NegEx_Form':
        original_source = row['Source_NegEx']
    else:
        original_source = ''

    separate_sources = [source.strip() for source in original_source.split(';')]
    parsed_sources = [
        (original, (citation, pages))
        for original, (citation, pages) in zip(
            separate_sources,
            map(citation_from_authoryear, separate_sources))
        if original]

    missing_sources = [
        original
        for original, (citation, _) in parsed_sources
        if not citation
        or ('p.c.' not in citation.replace(' ', '')
            and citation not in sources_by_citation)]
    matched_sources = (
        (citation, pages)
        for _, (citation, pages) in parsed_sources
        if citation
        and 'p.c.' not in citation.replace(' ', ''))

    for original in missing_sources:
        print(
            f'{language_id}:{param_col}: unknown source: {original}',
            file=sys.stderr)
    sources = [
        f'{entry.id}[{pages}]' if pages else entry.id
        for citation, pages in matched_sources
        if (entry := sources_by_citation.get(citation))]

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
        'Source': sources,
        'Source_comment': original_source,
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
    cldf.add_columns('ValueTable', 'Source_comment')


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
