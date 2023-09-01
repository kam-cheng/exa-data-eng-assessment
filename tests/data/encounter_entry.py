"""Example of a FHIR Encounter Entry from a FHIR Bundle file."""

ENCOUNTER_ENTRY = {
    "fullUrl": "urn:uuid:4dbc90e0-b7b2-482c-24af-1405654e59ae",
    "resource": {
        "resourceType": "Encounter",
        "id": "4dbc90e0-b7b2-482c-24af-1405654e59ae",
        "meta": {
            "profile": ["http://hl7.org/fhir/us/core/StructureDefinition/us-core-encounter"]
        },
        "identifier": [{
            "use": "official",
            "system": "https://github.com/synthetichealth/synthea",
            "value": "4dbc90e0-b7b2-482c-24af-1405654e59ae"
        }],
        "status": "finished",
        "class": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
            "code": "AMB"
        },
        "type": [{
            "coding": [{
                "system": "http://snomed.info/sct",
                "code": "162673000",
                "display": "General examination of patient (procedure)"
            }],
            "text": "General examination of patient (procedure)"
        }],
        "subject": {
            "reference": "urn:uuid:8c95253e-8ee8-9ae8-6d40-021d702dc78e",
            "display": "Mr. Aaron697 Dickens475"
        },
        "participant": [{
            "type": [{
                "coding": [{
                    "system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
                    "code": "PPRF",
                    "display": "primary performer"
                }],
                "text": "primary performer"
            }],
            "period": {
                "start": "1962-10-22T03:00:18+01:00",
                "end": "1962-10-22T03:15:18+01:00"
            },
            "individual": {
                "reference": "Practitioner?identifier=http://hl7.org/fhir/sid/us-npi|9999914519",
                "display": "Dr. Wilmer32 Heidenreich818"
            }
        }],
        "period": {
            "start": "1962-10-22T03:00:18+01:00",
            "end": "1962-10-22T03:15:18+01:00"
        },
        "location": [{
            "location": {
                "reference": "Location?identifier=https://github.com/synthetichealth/synthea|08770baf-f5e6-3a1a-af4f-1d8be70df56f",
                "display": "MASS LUNG AND ALLERGY PC"
            }
        }],
        "serviceProvider": {
            "reference": "Organization?identifier=https://github.com/synthetichealth/synthea|3f44c95d-aa3d-3385-93f7-9b0bdecc52a6",
            "display": "MASS LUNG AND ALLERGY PC"
        }
    },
    "request": {
        "method": "POST",
        "url": "Encounter"
    }
}
