from django.shortcuts import render

from rest_framework.views import APIView
from rest_framework.response import Response

from .models import CountryCases, CountryDeaths, CountryRecovered, CountryCritical, CountryTests, StateCases, StateDeaths, StateRecovered, StateTests, UsaTimeseriesBegin, UsaTimeseriesEnd

from . import plots


#----------------------
# Country data
#----------------------

#1.Confirmed Cases
cases_model = CountryCases.objects.all()
country_cases = []
country_confirmed_cases = []
for data in cases_model:
    country_cases.append(data.country)
    country_confirmed_cases.append(data.confirmed_cases)

#2.Deaths
deaths_model = CountryDeaths.objects.all()
country_deaths = []
country_total_deaths = []
for data in deaths_model:
    country_deaths.append(data.country)
    country_total_deaths.append(data.deaths)

#3.Recovered
recovered_model = CountryRecovered.objects.all()
country_recovered = []
country_total_recovered = []
for data in recovered_model:
    country_recovered.append(data.country)
    country_total_recovered.append(data.recovered)

#4.Critical
critical_model = CountryCritical.objects.all()
country_critical = []
country_total_critical = []
for data in critical_model:
    country_critical.append(data.country)
    country_total_critical.append(data.critical)

#5.Total Tests
tests_model = CountryTests.objects.all()
country_tests = []
country_total_tests = []
for data in tests_model:
    country_tests.append(data.country)
    country_total_tests.append(data.total_tests)

#----------------------
# State data
#----------------------

#1.Confirmed Cases
state_cases_model = StateCases.objects.all()
state_cases = []
state_confirmed_cases = []
for data in state_cases_model:
    state_cases.append(data.state)
    state_confirmed_cases.append(data.confirmed_cases)

#2.Deaths
state_deaths_model = StateDeaths.objects.all()
state_deaths = []
state_total_deaths = []
for data in state_deaths_model:
    state_deaths.append(data.state)
    state_total_deaths.append(data.deaths)

#3.Recovered
state_recovered_model = StateRecovered.objects.all()
state_recovered = []
state_total_recovered = []
for data in state_recovered_model:
    state_recovered.append(data.state)
    state_total_recovered.append(data.recovered)

#4.Total Tests
state_tests_model = StateTests.objects.all()
state_tests = []
state_total_tests = []
for data in state_tests_model:
    state_tests.append(data.state)
    state_total_tests.append(data.total_tests)

#----------------------
# USA Timeseries
#----------------------

timeseries_begin_model = UsaTimeseriesBegin.objects.all()
begin_date = []
begin_cases = []
begin_deaths = []
begin_recovered = []

for data in timeseries_begin_model:
    begin_date.append(data.date)
    begin_cases.append(data.confirmed_cases)
    begin_deaths.append(data.deaths)
    begin_recovered.append(data.recovered)


timeseries_end_model = UsaTimeseriesEnd.objects.all()
end_date = []
end_cases = []
end_deaths = []
end_recovered = []

for data in timeseries_end_model:
    end_date.append(data.date)
    end_cases.append(data.confirmed_cases)
    end_deaths.append(data.deaths)
    end_recovered.append(data.recovered)


# Create your views here.
def core_home(request):
    
    return render(request, 'index.html')

def core_usa(request):
    
    return render(request, 'usa.html')

def core_usamap(request):

    plot = plots.usaplot()
    
    context = {'plot':plot}

    return render(request, 'usamap.html', context)

def core_worldmap(request):

    plot = plots.worldplot()
    
    context = {'plot':plot}

    return render(request, 'worldmap.html', context)


class ChartData(APIView):

    authentication_classes = []
    permission_classes = []

    def get(self, request, format=None):

        data = {
            "country_cases": country_cases,
            "country_confirmed_cases": country_confirmed_cases,

            "country_deaths": country_deaths,
            "country_total_deaths": country_total_deaths,

            "country_recovered": country_recovered,
            "country_total_recovered": country_total_recovered,

            "country_critical": country_critical,
            "country_total_critical": country_total_critical,

            "country_tests": country_tests,
            "country_total_tests": country_total_tests,

            "state_cases": state_cases,
            "state_confirmed_cases": state_confirmed_cases,

            "state_deaths": state_deaths,
            "state_total_deaths": state_total_deaths,

            "state_recovered": state_recovered,
            "state_total_recovered": state_total_recovered,

            "state_tests": state_tests,
            "state_total_tests": state_total_tests,

            "begin_date": begin_date,
            "begin_cases": begin_cases,
            "begin_deaths": begin_deaths,
            "begin_recovered": begin_recovered,

            "end_date": end_date,
            "end_cases": end_cases,
            "end_deaths": end_deaths,
            "end_recovered": end_recovered,
  
        }
        return Response(data)